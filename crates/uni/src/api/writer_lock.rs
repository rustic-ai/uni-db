// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Holding a store for writing: the OS lock, the object-store lease, or a
//! custom [`WriteLeaseProvider`], chosen by [`WriteLease`].

use std::sync::Arc;

use uni_common::{Result, UniError};
use uni_store::lease::{self, LeaseSettings, WriteFence};
use uni_store::location::StoreLocation;

use super::multi_agent::{LeaseGuard, WriteLease, WriteLeaseProvider};

/// How long a drop waits to give the store back before leaving the lease to
/// lapse.
const RELEASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// What a writable handle holds.
enum WriterGuard {
    Local(lease::LocalDirLock),
    Remote(Arc<lease::RemoteLease>),
    Custom {
        provider: Arc<dyn WriteLeaseProvider>,
        guard: Option<LeaseGuard>,
        heartbeat: tokio::task::JoinHandle<()>,
    },
}

/// A writable handle's hold on its store, released once.
pub(crate) struct WriterHold {
    guards: parking_lot::Mutex<Vec<WriterGuard>>,
    fence: Arc<WriteFence>,
}

impl Drop for WriterHold {
    /// A hold dropped without an explicit release (a failed open, say)
    /// still gives the store back.
    fn drop(&mut self) {
        if !self.guards.get_mut().is_empty() {
            self.release_blocking();
        }
    }
}

impl WriterHold {
    /// Gives the store up. The handle refuses writes from here on, so a
    /// lingering session cannot write behind the next writer's back.
    pub(crate) async fn release(&self) {
        self.fence.trip("the handle was closed");
        let guards: Vec<WriterGuard> = std::mem::take(&mut *self.guards.lock());
        for guard in guards {
            match guard {
                WriterGuard::Local(lock) => drop(lock),
                WriterGuard::Remote(lease) => lease.release().await,
                WriterGuard::Custom {
                    provider,
                    guard,
                    heartbeat,
                } => {
                    heartbeat.abort();
                    if let Some(guard) = guard
                        && let Err(e) = provider.release(guard).await
                    {
                        tracing::warn!(error = %e, "write lease provider failed to release");
                    }
                }
            }
        }
    }

    /// [`release`](Self::release) from a synchronous context (a drop).
    pub(crate) fn release_blocking(&self) {
        self.fence.trip("the handle was closed");
        let guards: Vec<WriterGuard> = std::mem::take(&mut *self.guards.lock());
        let (local, io): (Vec<_>, Vec<_>) = guards
            .into_iter()
            .partition(|g| matches!(g, WriterGuard::Local(_)));
        drop(local);
        if io.is_empty() {
            return;
        }
        let this = Self {
            guards: parking_lot::Mutex::new(io),
            fence: Arc::clone(&self.fence),
        };
        // Bounded: a drop must never hang on the store. A lease not given
        // back lapses on its own after its TTL.
        let release = async move {
            if tokio::time::timeout(RELEASE_TIMEOUT, this.release())
                .await
                .is_err()
            {
                tracing::warn!("could not release the writer lease in time; it lapses on its own");
            }
        };
        // The store's connections are driven by the runtime that opened them,
        // so the release runs where it can finish:
        //
        // * a multi-threaded runtime: blocked on in place, on that runtime;
        // * a `current_thread` runtime: that runtime is the thread blocked in
        //   this drop, so a request waits on it until the drop returns. The
        //   release is handed to the process-wide IO runtime and not waited
        //   for; it completes once the caller's runtime runs again, which a
        //   reopen's acquire grace allows for;
        // * no runtime: on the process-wide IO runtime, waited for.
        match tokio::runtime::Handle::try_current().map(|h| (h.runtime_flavor(), h)) {
            Ok((tokio::runtime::RuntimeFlavor::MultiThread, handle)) => {
                tokio::task::block_in_place(|| handle.block_on(release));
            }
            Ok(_) => match uni_store::runtime::io_runtime::io_runtime() {
                Ok(rt) => drop(rt.spawn(release)),
                Err(e) => {
                    tracing::warn!(error = %e, "could not release the writer lease; it lapses on its own")
                }
            },
            Err(_) => {
                let _ = uni_store::runtime::io_runtime::block_on_io_runtime(async move {
                    release.await;
                    Ok::<(), anyhow::Error>(())
                });
            }
        }
    }
}

/// Delegates to a shared provider, so the handle can keep reporting its
/// `WriteLease::Custom` while the hold calls the same provider.
struct SharedProvider(Arc<dyn WriteLeaseProvider>);

#[async_trait::async_trait]
impl WriteLeaseProvider for SharedProvider {
    async fn acquire(&self) -> Result<LeaseGuard> {
        self.0.acquire().await
    }
    async fn heartbeat(&self, guard: &LeaseGuard) -> Result<()> {
        self.0.heartbeat(guard).await
    }
    async fn release(&self, guard: LeaseGuard) -> Result<()> {
        self.0.release(guard).await
    }
}

/// Takes the store for writing before anything is written to it.
///
/// `lease` is the configured strategy; a `Custom` provider is moved into the
/// hold and replaced by a delegate, so the handle still reports it.
pub(crate) async fn acquire(
    lease: &mut Option<WriteLease>,
    location: &StoreLocation,
    local_dir: Option<&std::path::Path>,
    settings: LeaseSettings,
    fence: &Arc<WriteFence>,
) -> Result<Option<Arc<WriterHold>>> {
    let locked = |e: anyhow::Error| match e.downcast::<lease::StoreLocked>() {
        Ok(locked) => UniError::StoreLocked {
            store: locked.store,
            holder: locked.holder,
        },
        Err(e) => UniError::Internal(e),
    };
    if settings.renew_interval.is_zero() || settings.renew_interval >= settings.ttl {
        return Err(UniError::Internal(anyhow::anyhow!(
            "UniConfig::writer_lease.renew_interval ({:?}) must be above zero and below its \
             ttl ({:?}), or the lease lapses between renewals",
            settings.renew_interval,
            settings.ttl
        )));
    }
    // A hybrid store's local directory (its WAL and ID allocator) is held
    // too, so the same directory cannot be opened as a plain local store.
    let mut held: Vec<WriterGuard> = Vec::new();
    if let Some(dir) = local_dir {
        let lock = lease::acquire_local(dir, settings.acquire_grace)
            .await
            .map_err(locked)?;
        held.push(WriterGuard::Local(lock));
    }
    let hold = move |guard: WriterGuard| {
        held.push(guard);
        Ok(Some(Arc::new(WriterHold {
            guards: parking_lot::Mutex::new(held),
            fence: Arc::clone(fence),
        })))
    };
    match lease.take() {
        Some(WriteLease::Unenforced) => {
            tracing::warn!(
                store = location.root_uri(),
                "WriteLease::Unenforced: no lock or lease is taken; open at most one writable \
                 handle on this store"
            );
            *lease = Some(WriteLease::Unenforced);
            Ok(None)
        }
        Some(WriteLease::DynamoDB { table }) => Err(UniError::Internal(anyhow::anyhow!(
            "WriteLease::DynamoDB (table '{table}') is not implemented; use the built-in lease \
             (WriteLease::Local, the default) or a WriteLease::Custom provider"
        ))),
        Some(WriteLease::Custom(provider)) => {
            let provider: Arc<dyn WriteLeaseProvider> = Arc::from(provider);
            *lease = Some(WriteLease::Custom(Box::new(SharedProvider(Arc::clone(
                &provider,
            )))));
            let guard = provider.acquire().await?;
            let heartbeat = {
                let provider = Arc::clone(&provider);
                let fence = Arc::clone(fence);
                let guard = LeaseGuard {
                    lease_id: guard.lease_id.clone(),
                    expires_at: guard.expires_at,
                };
                let interval = settings.renew_interval;
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(interval).await;
                        if fence.is_tripped() {
                            return;
                        }
                        if let Err(e) = provider.heartbeat(&guard).await {
                            fence.trip(format!("its write lease provider lost the lease: {e}"));
                            return;
                        }
                    }
                })
            };
            hold(WriterGuard::Custom {
                provider,
                guard: Some(guard),
                heartbeat,
            })
        }
        other @ (None | Some(WriteLease::Local)) => {
            *lease = other;
            // A store in a local directory (a plain path or `file://`) is held
            // by an OS lock; an object store (a hybrid store's data included)
            // by a lease.
            match location.fs_root() {
                Some(dir) => {
                    let lock = lease::acquire_local(dir, settings.acquire_grace)
                        .await
                        .map_err(locked)?;
                    hold(WriterGuard::Local(lock))
                }
                None => {
                    let held = lease::acquire_remote(
                        location.meta_store(),
                        location.root_uri(),
                        settings,
                        Arc::clone(fence),
                    )
                    .await
                    .map_err(locked)?;
                    held.start_renewal();
                    hold(WriterGuard::Remote(held))
                }
            }
        }
    }
}
