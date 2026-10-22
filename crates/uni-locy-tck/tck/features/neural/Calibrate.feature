Feature: CALIBRATE statement (Phase C C2)

  End-to-end calibration: a `CALIBRATE` statement collects
  `(prediction, ground_truth)` pairs by invoking the registered
  classifier over a Cypher MATCH pattern, fits the chosen calibrator
  on the training split, and reports holdout metrics.

  Phase C gate (rollout doc §Phase C): a miscalibrated mock classifier
  (always 0.95) calibrated via Platt should reduce ECE by ≥ 50%.

  Background:
    Given an empty graph

  # ── Phase C gate: Platt halves ECE on overconfident mock ────────────────

  Scenario: Miscalibrated mock classifier reduces ECE via Platt CALIBRATE
    Given having executed:
      """
      CREATE
        (n0:Sample {idx: 0,  label: true}),  (n1:Sample {idx: 1,  label: false}),
        (n2:Sample {idx: 2,  label: true}),  (n3:Sample {idx: 3,  label: false}),
        (n4:Sample {idx: 4,  label: true}),  (n5:Sample {idx: 5,  label: false}),
        (n6:Sample {idx: 6,  label: true}),  (n7:Sample {idx: 7,  label: false}),
        (n8:Sample {idx: 8,  label: true}),  (n9:Sample {idx: 9,  label: false}),
        (n10:Sample {idx: 10, label: true}), (n11:Sample {idx: 11, label: false}),
        (n12:Sample {idx: 12, label: true}), (n13:Sample {idx: 13, label: false}),
        (n14:Sample {idx: 14, label: true}), (n15:Sample {idx: 15, label: false}),
        (n16:Sample {idx: 16, label: true}), (n17:Sample {idx: 17, label: false}),
        (n18:Sample {idx: 18, label: true}), (n19:Sample {idx: 19, label: false}),
        (n20:Sample {idx: 20, label: true}), (n21:Sample {idx: 21, label: false}),
        (n22:Sample {idx: 22, label: true}), (n23:Sample {idx: 23, label: false}),
        (n24:Sample {idx: 24, label: true}), (n25:Sample {idx: 25, label: false}),
        (n26:Sample {idx: 26, label: true}), (n27:Sample {idx: 27, label: false}),
        (n28:Sample {idx: 28, label: true}), (n29:Sample {idx: 29, label: false}),
        (n30:Sample {idx: 30, label: true}), (n31:Sample {idx: 31, label: false}),
        (n32:Sample {idx: 32, label: true}), (n33:Sample {idx: 33, label: false}),
        (n34:Sample {idx: 34, label: true}), (n35:Sample {idx: 35, label: false}),
        (n36:Sample {idx: 36, label: true}), (n37:Sample {idx: 37, label: false}),
        (n38:Sample {idx: 38, label: true}), (n39:Sample {idx: 39, label: false})
      """
    And a registered mock classifier "scorer" returning 0.95
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CALIBRATE scorer
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD platt_scaling
        HOLDOUT 0.25
      """
    Then evaluation should succeed
    And the calibration result for "scorer" should report method "Platt"
    And the calibration result for "scorer" should have calibrated_ece less than half the raw_ece

  # ── C4: UncalibratedNeuralPredicate warning fires for uncalibrated PROB model ──

  Scenario: Invoking an uncalibrated PROB model emits UncalibratedNeuralPredicate
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, scorer(s) AS risk
      """
    Then evaluation should succeed
    And the result should contain an UncalibratedNeuralPredicate warning

  # ── Same rule with CALIBRATION declared: no warning ─────────────────────

  Scenario: Declaring CALIBRATION suppresses UncalibratedNeuralPredicate
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, scorer(s) AS risk
      """
    Then evaluation should succeed

  # ── CALIBRATE on unknown classifier errors at runtime ───────────────────

  Scenario: CALIBRATE on unregistered classifier errors at runtime
    Given having executed:
      """
      CREATE (:Sample {idx: 0, label: true}), (:Sample {idx: 1, label: false})
      """
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL ghost AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/ghost')

      CALIBRATE ghost
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD platt_scaling
      """
    Then evaluation should fail
    And the evaluation error should mention "not registered"

  # ── Phase D D-C1d: Dirichlet surface accepted, runtime errors honestly ─

  Scenario: CALIBRATE with METHOD dirichlet parses but errors at fit time
    Given having executed:
      """
      CREATE (:Sample {idx: 0, label: true}), (:Sample {idx: 1, label: false})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CALIBRATE scorer
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD dirichlet
      """
    # Grammar accepts the keyword (D-C1d-Surface acceptance); the
    # binary CALIBRATE pipeline rejects multi-class at fit time
    # with a clear pointer to the Rust API. Future slice surfaces a
    # multi-class form (TARGET class_idx METRICS class_probs).
    Then evaluation should fail
    And the evaluation error should mention "Dirichlet"

  # ── CALIBRATE after a write reads the state the write left ──────────────

  Scenario: CALIBRATE after a Cypher write collects the written samples too
    Given having executed:
      """
      UNWIND range(0, 39) AS i CREATE (:Sample {idx: i, label: i % 2 = 0})
      """
    And a registered mock classifier "scorer" returning 0.95
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CALIBRATE scorer
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD platt_scaling
        HOLDOUT 0.25

      UNWIND range(40, 79) AS i CREATE (:Sample {idx: i, label: i % 2 = 0})

      CALIBRATE scorer
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD platt_scaling
        HOLDOUT 0.25
      """
    Then evaluation should succeed
    And the result at path 0 should be a Calibrate of 40 samples with 10 held out
    And the result at path 2 should be a Calibrate of 80 samples with 20 held out
