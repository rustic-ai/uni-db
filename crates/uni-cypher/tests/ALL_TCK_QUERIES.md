// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.doNothing()

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.doNothing

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
MATCH (n)
CALL test.doNothing()
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
MATCH (n)
CALL test.doNothing()
RETURN n.name AS `name`

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.labels()

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.labels() YIELD label
RETURN label

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc('Dobby')

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc('Dobby') YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc(1, 2, 3, 4)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc(1, 2, 3, 4) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc(1)
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
CALL test.my.proc() YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
WITH 'Hi' AS label
CALL test.labels() YIELD label
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call1.feature
MATCH (n)
CALL test.labels(count(n)) YIELD label
RETURN label

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
CALL test.my.proc('Stefan', 1) YIELD city, country_code
RETURN city, country_code

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
CALL test.my.proc('Stefan', 1)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
CALL test.my.proc

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
CALL test.my.proc YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
CALL test.my.proc(true)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call2.feature
CALL test.my.proc(true) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call3.feature
CALL test.my.proc(42)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call3.feature
CALL test.my.proc(42) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call3.feature
CALL test.my.proc(42.3)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call3.feature
CALL test.my.proc(42.3) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call3.feature
CALL test.my.proc(42)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call3.feature
CALL test.my.proc(42) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call4.feature
CALL test.my.proc(null)

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call4.feature
CALL test.my.proc(null) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD out
RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD out
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a, b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD b, a
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS c, b AS d
RETURN c, d

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS b, b AS d
RETURN b, d

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS c, b AS a
RETURN c, a

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS b, b AS a
RETURN b, a

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS c, b AS b
RETURN c, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS c, b
RETURN c, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS a, b AS d
RETURN a, d

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a, b AS d
RETURN a, d

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS a, b AS b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS a, b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a, b AS b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a, b AS a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc(null) YIELD a AS c, b AS c
RETURN c

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc('Stefan', 1) YIELD *
RETURN city, country_code

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call5.feature
CALL test.my.proc('Stefan', 1) YIELD *

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call6.feature
CALL test.labels() YIELD label
WITH count(*) AS c
CALL test.labels() YIELD label
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call6.feature
CALL test.my.proc(null) YIELD out
WITH out RETURN out

// ../../cypher-tck/tck-M23/tck/features/clauses/call/Call6.feature
CALL test.my.proc(null) YIELD out
WITH out AS a RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE ()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (), ()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (:Label)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (:Label), (:Label)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (:A:B:C:D)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (:B:A:D), (:B:C), (:D:E:B)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE ({created: true})

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n {name: 'foo'})
RETURN n.name AS p

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n {id: 12, name: 'foo'})

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n {id: 12, name: 'foo'})
RETURN n.id AS id, n.name AS p

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n {id: 12, name: null})
RETURN n.id AS id, n.name AS p

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (p:TheLabel {id: 4611686018427387905})
RETURN p.id

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
MATCH (a)
CREATE (a)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
MATCH (a)
CREATE (a {name: 'foo'})
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n:Foo)-[:T1]->(),
       (n:Bar)-[:T2]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE ()<-[:T2]-(n:Foo),
       (n:Bar)<-[:T1]-()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n:Foo)
CREATE (n:Bar)-[:OWNS]->(:Dog)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n {})
CREATE (n:Bar)-[:OWNS]->(:Dog)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (n:Foo)
CREATE (n {})-[:OWNS]->(:Dog)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create1.feature
CREATE (b {name: missing})
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[:R]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (a), (b),
       (a)-[:R]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (a)
CREATE (b)
CREATE (a)-[:R]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (:A)<-[:R]-(:B)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH (x:X), (y:Y)
CREATE (x)-[:R]->(y)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH (x:X), (y:Y)
CREATE (x)<-[:R]-(y)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (root)-[:LINK]->(root)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (root),
       (root)-[:LINK]->(root)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (root)
CREATE (root)-[:LINK]->(root)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH (root:Root)
CREATE (root)-[:LINK]->(root)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH (x:Begin)
CREATE (x)-[:TYPE]->(:End)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH (x:End)
CREATE (:Begin)-[:TYPE]->(x)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[:R {num: 42}]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[r:R {num: 42}]->()
RETURN r.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[:R {id: 12, name: 'foo'}]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[r:R {id: 12, name: 'foo'}]->()
RETURN r.id AS id, r.name AS name

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[r:X {id: 12, name: null}]->()
RETURN r.id, r.name AS name

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (a)-[:FOO]-(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE (a)<-[:FOO]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[:A|:B]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
CREATE ()-[:FOO*2]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH ()-[r]->()
CREATE ()-[r]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create2.feature
MATCH (a)
CREATE (a)-[:KNOWS]->(b {name: missing})
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH ()
CREATE ()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH ()
CREATE ()
WITH *
CREATE ()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH ()
CREATE ()
WITH *
MATCH ()
CREATE ()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH ()
CREATE ()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH (n)
MATCH (m)
WITH n AS a, m AS b
CREATE (a)-[:T]->(b)
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH (n)
WITH n AS a
CREATE (a)-[:T]->()
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH (n)
MATCH (m)
WITH n AS a, m AS b
CREATE (a)-[:T]->(b)
WITH a AS x, b AS y
CREATE (x)-[:T]->(y)
RETURN x, y

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MATCH (n)
WITH n AS a
CREATE (a)-[:T]->()
WITH a AS x
CREATE (x)-[:T]->()
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
CREATE (a)
WITH a
WITH *
CREATE (b)
CREATE (a)<-[:T]-(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
CREATE (a)
WITH a
UNWIND [0] AS i
CREATE (b)
CREATE (a)<-[:T]-(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
CREATE (a)
WITH a
MERGE ()
CREATE (b)
CREATE (a)<-[:T]-(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
CREATE (a)
WITH a
MERGE (x)
MERGE (y)
MERGE (x)-[:T]->(y)
CREATE (b)
CREATE (a)<-[:T]-(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create3.feature
MERGE (t:T {id: 42})
CREATE (f:R)
CREATE (t)-[:REL]->(f)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create4.feature
CREATE (theMatrix:Movie {title: 'The Matrix', released: 1999, tagline: 'Welcome to the Real World'})
CREATE (keanu:Person {name: 'Keanu Reeves', born: 1964})
CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967})
CREATE (laurence:Person {name: 'Laurence Fishburne', born: 1961})
CREATE (hugo:Person {name: 'Hugo Weaving', born: 1960})
CREATE (andyW:Person {name: 'Andy Wachowski', born: 1967})
CREATE (lanaW:Person {name: 'Lana Wachowski', born: 1965})
CREATE (joelS:Person {name: 'Joel Silver', born: 1952})
CREATE
  (keanu)-[:ACTED_IN {roles: ['Neo']}]->(theMatrix),
  (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrix),
  (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(theMatrix),
  (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(theMatrix),
  (andyW)-[:DIRECTED]->(theMatrix),
  (lanaW)-[:DIRECTED]->(theMatrix),
  (joelS)-[:PRODUCED]->(theMatrix)
CREATE (emil:Person {name: 'Emil Eifrem', born: 1978})
CREATE (emil)-[:ACTED_IN {roles: ['Emil']}]->(theMatrix)
CREATE (theMatrixReloaded:Movie {title: 'The Matrix Reloaded', released: 2003,
        tagline: 'Free your mind'})
CREATE
  (keanu)-[:ACTED_IN {roles: ['Neo'] }]->(theMatrixReloaded),
  (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrixReloaded),
  (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(theMatrixReloaded),
  (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(theMatrixReloaded),
  (andyW)-[:DIRECTED]->(theMatrixReloaded),
  (lanaW)-[:DIRECTED]->(theMatrixReloaded),
  (joelS)-[:PRODUCED]->(theMatrixReloaded)
CREATE (theMatrixRevolutions:Movie {title: 'The Matrix Revolutions', released: 2003,
  tagline: 'Everything that has a beginning has an end'})
CREATE
  (keanu)-[:ACTED_IN {roles: ['Neo']}]->(theMatrixRevolutions),
  (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrixRevolutions),
  (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(theMatrixRevolutions),
  (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(theMatrixRevolutions),
  (andyW)-[:DIRECTED]->(theMatrixRevolutions),
  (lanaW)-[:DIRECTED]->(theMatrixRevolutions),
  (joelS)-[:PRODUCED]->(theMatrixRevolutions)
CREATE (theDevilsAdvocate:Movie {title: 'The Devil\'s Advocate', released: 1997,
  tagline: 'Evil has its winning ways'})
CREATE (charlize:Person {name: 'Charlize Theron', born: 1975})
CREATE (al:Person {name: 'Al Pacino', born: 1940})
CREATE (taylor:Person {name: 'Taylor Hackford', born: 1944})
CREATE
  (keanu)-[:ACTED_IN {roles: ['Kevin Lomax']}]->(theDevilsAdvocate),
  (charlize)-[:ACTED_IN {roles: ['Mary Ann Lomax']}]->(theDevilsAdvocate),
  (al)-[:ACTED_IN {roles: ['John Milton']}]->(theDevilsAdvocate),
  (taylor)-[:DIRECTED]->(theDevilsAdvocate)
CREATE (aFewGoodMen:Movie {title: 'A Few Good Men', released: 1992,
  tagline: 'Deep within the heart of the nation\'s capital, one man will stop at nothing to keep his honor, ...'})
CREATE (tomC:Person {name: 'Tom Cruise', born: 1962})
CREATE (jackN:Person {name: 'Jack Nicholson', born: 1937})
CREATE (demiM:Person {name: 'Demi Moore', born: 1962})
CREATE (kevinB:Person {name: 'Kevin Bacon', born: 1958})
CREATE (kieferS:Person {name: 'Kiefer Sutherland', born: 1966})
CREATE (noahW:Person {name: 'Noah Wyle', born: 1971})
CREATE (cubaG:Person {name: 'Cuba Gooding Jr.', born: 1968})
CREATE (kevinP:Person {name: 'Kevin Pollak', born: 1957})
CREATE (jTW:Person {name: 'J.T. Walsh', born: 1943})
CREATE (jamesM:Person {name: 'James Marshall', born: 1967})
CREATE (christopherG:Person {name: 'Christopher Guest', born: 1948})
CREATE (robR:Person {name: 'Rob Reiner', born: 1947})
CREATE (aaronS:Person {name: 'Aaron Sorkin', born: 1961})
CREATE
  (tomC)-[:ACTED_IN {roles: ['Lt. Daniel Kaffee']}]->(aFewGoodMen),
  (jackN)-[:ACTED_IN {roles: ['Col. Nathan R. Jessup']}]->(aFewGoodMen),
  (demiM)-[:ACTED_IN {roles: ['Lt. Cdr. JoAnne Galloway']}]->(aFewGoodMen),
  (kevinB)-[:ACTED_IN {roles: ['Capt. Jack Ross']}]->(aFewGoodMen),
  (kieferS)-[:ACTED_IN {roles: ['Lt. Jonathan Kendrick']}]->(aFewGoodMen),
  (noahW)-[:ACTED_IN {roles: ['Cpl. Jeffrey Barnes']}]->(aFewGoodMen),
  (cubaG)-[:ACTED_IN {roles: ['Cpl. Carl Hammaker']}]->(aFewGoodMen),
  (kevinP)-[:ACTED_IN {roles: ['Lt. Sam Weinberg']}]->(aFewGoodMen),
  (jTW)-[:ACTED_IN {roles: ['Lt. Col. Matthew Andrew Markinson']}]->(aFewGoodMen),
  (jamesM)-[:ACTED_IN {roles: ['Pfc. Louden Downey']}]->(aFewGoodMen),
  (christopherG)-[:ACTED_IN {roles: ['Dr. Stone']}]->(aFewGoodMen),
  (aaronS)-[:ACTED_IN {roles: ['Bar patron']}]->(aFewGoodMen),
  (robR)-[:DIRECTED]->(aFewGoodMen),
  (aaronS)-[:WROTE]->(aFewGoodMen)
CREATE (topGun:Movie {title: 'Top Gun', released: 1986,
    tagline: 'I feel the need, the need for speed.'})
CREATE (kellyM:Person {name: 'Kelly McGillis', born: 1957})
CREATE (valK:Person {name: 'Val Kilmer', born: 1959})
CREATE (anthonyE:Person {name: 'Anthony Edwards', born: 1962})
CREATE (tomS:Person {name: 'Tom Skerritt', born: 1933})
CREATE (megR:Person {name: 'Meg Ryan', born: 1961})
CREATE (tonyS:Person {name: 'Tony Scott', born: 1944})
CREATE (jimC:Person {name: 'Jim Cash', born: 1941})
CREATE
  (tomC)-[:ACTED_IN {roles: ['Maverick']}]->(topGun),
  (kellyM)-[:ACTED_IN {roles: ['Charlie']}]->(topGun),
  (valK)-[:ACTED_IN {roles: ['Iceman']}]->(topGun),
  (anthonyE)-[:ACTED_IN {roles: ['Goose']}]->(topGun),
  (tomS)-[:ACTED_IN {roles: ['Viper']}]->(topGun),
  (megR)-[:ACTED_IN {roles: ['Carole']}]->(topGun),
  (tonyS)-[:DIRECTED]->(topGun),
  (jimC)-[:WROTE]->(topGun)
CREATE (jerryMaguire:Movie {title: 'Jerry Maguire', released: 2000,
    tagline: 'The rest of his life begins now.'})
CREATE (reneeZ:Person {name: 'Renee Zellweger', born: 1969})
CREATE (kellyP:Person {name: 'Kelly Preston', born: 1962})
CREATE (jerryO:Person {name: 'Jerry O\'Connell', born: 1974})
CREATE (jayM:Person {name: 'Jay Mohr', born: 1970})
CREATE (bonnieH:Person {name: 'Bonnie Hunt', born: 1961})
CREATE (reginaK:Person {name: 'Regina King', born: 1971})
CREATE (jonathanL:Person {name: 'Jonathan Lipnicki', born: 1996})
CREATE (cameronC:Person {name: 'Cameron Crowe', born: 1957})
CREATE
  (tomC)-[:ACTED_IN {roles: ['Jerry Maguire']}]->(jerryMaguire),
  (cubaG)-[:ACTED_IN {roles: ['Rod Tidwell']}]->(jerryMaguire),
  (reneeZ)-[:ACTED_IN {roles: ['Dorothy Boyd']}]->(jerryMaguire),
  (kellyP)-[:ACTED_IN {roles: ['Avery Bishop']}]->(jerryMaguire),
  (jerryO)-[:ACTED_IN {roles: ['Frank Cushman']}]->(jerryMaguire),
  (jayM)-[:ACTED_IN {roles: ['Bob Sugar']}]->(jerryMaguire),
  (bonnieH)-[:ACTED_IN {roles: ['Laurel Boyd']}]->(jerryMaguire),
  (reginaK)-[:ACTED_IN {roles: ['Marcee Tidwell']}]->(jerryMaguire),
  (jonathanL)-[:ACTED_IN {roles: ['Ray Boyd']}]->(jerryMaguire),
  (cameronC)-[:DIRECTED]->(jerryMaguire),
  (cameronC)-[:PRODUCED]->(jerryMaguire),
  (cameronC)-[:WROTE]->(jerryMaguire)
CREATE (standByMe:Movie {title: 'Stand-By-Me', released: 1986,
    tagline: 'The last real taste of innocence'})
CREATE (riverP:Person {name: 'River Phoenix', born: 1970})
CREATE (coreyF:Person {name: 'Corey Feldman', born: 1971})
CREATE (wilW:Person {name: 'Wil Wheaton', born: 1972})
CREATE (johnC:Person {name: 'John Cusack', born: 1966})
CREATE (marshallB:Person {name: 'Marshall Bell', born: 1942})
CREATE
  (wilW)-[:ACTED_IN {roles: ['Gordie Lachance']}]->(standByMe),
  (riverP)-[:ACTED_IN {roles: ['Chris Chambers']}]->(standByMe),
  (jerryO)-[:ACTED_IN {roles: ['Vern Tessio']}]->(standByMe),
  (coreyF)-[:ACTED_IN {roles: ['Teddy Duchamp']}]->(standByMe),
  (johnC)-[:ACTED_IN {roles: ['Denny Lachance']}]->(standByMe),
  (kieferS)-[:ACTED_IN {roles: ['Ace Merrill']}]->(standByMe),
  (marshallB)-[:ACTED_IN {roles: ['Mr. Lachance']}]->(standByMe),
  (robR)-[:DIRECTED]->(standByMe)
CREATE (asGoodAsItGets:Movie {title: 'As-good-as-it-gets', released: 1997,
    tagline: 'A comedy from the heart that goes for the throat'})
CREATE (helenH:Person {name: 'Helen Hunt', born: 1963})
CREATE (gregK:Person {name: 'Greg Kinnear', born: 1963})
CREATE (jamesB:Person {name: 'James L. Brooks', born: 1940})
CREATE
  (jackN)-[:ACTED_IN {roles: ['Melvin Udall']}]->(asGoodAsItGets),
  (helenH)-[:ACTED_IN {roles: ['Carol Connelly']}]->(asGoodAsItGets),
  (gregK)-[:ACTED_IN {roles: ['Simon Bishop']}]->(asGoodAsItGets),
  (cubaG)-[:ACTED_IN {roles: ['Frank Sachs']}]->(asGoodAsItGets),
  (jamesB)-[:DIRECTED]->(asGoodAsItGets)
CREATE (whatDreamsMayCome:Movie {title: 'What Dreams May Come', released: 1998,
    tagline: 'After life there is more. The end is just the beginning.'})
CREATE (annabellaS:Person {name: 'Annabella Sciorra', born: 1960})
CREATE (maxS:Person {name: 'Max von Sydow', born: 1929})
CREATE (wernerH:Person {name: 'Werner Herzog', born: 1942})
CREATE (robin:Person {name: 'Robin Williams', born: 1951})
CREATE (vincentW:Person {name: 'Vincent Ward', born: 1956})
CREATE
  (robin)-[:ACTED_IN {roles: ['Chris Nielsen']}]->(whatDreamsMayCome),
  (cubaG)-[:ACTED_IN {roles: ['Albert Lewis']}]->(whatDreamsMayCome),
  (annabellaS)-[:ACTED_IN {roles: ['Annie Collins-Nielsen']}]->(whatDreamsMayCome),
  (maxS)-[:ACTED_IN {roles: ['The Tracker']}]->(whatDreamsMayCome),
  (wernerH)-[:ACTED_IN {roles: ['The Face']}]->(whatDreamsMayCome),
  (vincentW)-[:DIRECTED]->(whatDreamsMayCome)
CREATE (snowFallingonCedars:Movie {title: 'Snow-Falling-on-Cedars', released: 1999,
  tagline: 'First loves last. Forever.'})
CREATE (ethanH:Person {name: 'Ethan Hawke', born: 1970})
CREATE (rickY:Person {name: 'Rick Yune', born: 1971})
CREATE (jamesC:Person {name: 'James Cromwell', born: 1940})
CREATE (scottH:Person {name: 'Scott Hicks', born: 1953})
CREATE
  (ethanH)-[:ACTED_IN {roles: ['Ishmael Chambers']}]->(snowFallingonCedars),
  (rickY)-[:ACTED_IN {roles: ['Kazuo Miyamoto']}]->(snowFallingonCedars),
  (maxS)-[:ACTED_IN {roles: ['Nels Gudmundsson']}]->(snowFallingonCedars),
  (jamesC)-[:ACTED_IN {roles: ['Judge Fielding']}]->(snowFallingonCedars),
  (scottH)-[:DIRECTED]->(snowFallingonCedars)
CREATE (youveGotMail:Movie {title: 'You\'ve Got Mail', released: 1998,
    tagline: 'At-odds-in-life, in-love-on-line'})
CREATE (parkerP:Person {name: 'Parker Posey', born: 1968})
CREATE (daveC:Person {name: 'Dave Chappelle', born: 1973})
CREATE (steveZ:Person {name: 'Steve Zahn', born: 1967})
CREATE (tomH:Person {name: 'Tom Hanks', born: 1956})
CREATE (noraE:Person {name: 'Nora Ephron', born: 1941})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Joe Fox']}]->(youveGotMail),
  (megR)-[:ACTED_IN {roles: ['Kathleen Kelly']}]->(youveGotMail),
  (gregK)-[:ACTED_IN {roles: ['Frank Navasky']}]->(youveGotMail),
  (parkerP)-[:ACTED_IN {roles: ['Patricia Eden']}]->(youveGotMail),
  (daveC)-[:ACTED_IN {roles: ['Kevin Jackson']}]->(youveGotMail),
  (steveZ)-[:ACTED_IN {roles: ['George Pappas']}]->(youveGotMail),
  (noraE)-[:DIRECTED]->(youveGotMail)
CREATE (sleeplessInSeattle:Movie {title: 'Sleepless-in-Seattle', released: 1993,
    tagline: 'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})
CREATE (ritaW:Person {name: 'Rita Wilson', born: 1956})
CREATE (billPull:Person {name: 'Bill Pullman', born: 1953})
CREATE (victorG:Person {name: 'Victor Garber', born: 1949})
CREATE (rosieO:Person {name: 'Rosie O\'Donnell', born: 1962})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Sam Baldwin']}]->(sleeplessInSeattle),
  (megR)-[:ACTED_IN {roles: ['Annie Reed']}]->(sleeplessInSeattle),
  (ritaW)-[:ACTED_IN {roles: ['Suzy']}]->(sleeplessInSeattle),
  (billPull)-[:ACTED_IN {roles: ['Walter']}]->(sleeplessInSeattle),
  (victorG)-[:ACTED_IN {roles: ['Greg']}]->(sleeplessInSeattle),
  (rosieO)-[:ACTED_IN {roles: ['Becky']}]->(sleeplessInSeattle),
  (noraE)-[:DIRECTED]->(sleeplessInSeattle)
CREATE (joeVersustheVolcano:Movie {title: 'Joe-Versus-the-Volcano', released: 1990,
    tagline: 'A story of love'})
CREATE (johnS:Person {name: 'John Patrick Stanley', born: 1950})
CREATE (nathan:Person {name: 'Nathan Lane', born: 1956})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Joe Banks']}]->(joeVersustheVolcano),
  (megR)-[:ACTED_IN {roles: ['DeDe', 'Angelica Graynamore', 'Patricia Graynamore']}]->(joeVersustheVolcano),
  (nathan)-[:ACTED_IN {roles: ['Baw']}]->(joeVersustheVolcano),
  (johnS)-[:DIRECTED]->(joeVersustheVolcano)
CREATE (whenHarryMetSally:Movie {title: 'When-Harry-Met-Sally', released: 1998,
    tagline: 'When-Harry-Met-Sally'})
CREATE (billyC:Person {name: 'Billy Crystal', born: 1948})
CREATE (carrieF:Person {name: 'Carrie Fisher', born: 1956})
CREATE (brunoK:Person {name: 'Bruno Kirby', born: 1949})
CREATE
  (billyC)-[:ACTED_IN {roles: ['Harry Burns']}]->(whenHarryMetSally),
  (megR)-[:ACTED_IN {roles: ['Sally Albright']}]->(whenHarryMetSally),
  (carrieF)-[:ACTED_IN {roles: ['Marie']}]->(whenHarryMetSally),
  (brunoK)-[:ACTED_IN {roles: ['Jess']}]->(whenHarryMetSally),
  (robR)-[:DIRECTED]->(whenHarryMetSally),
  (robR)-[:PRODUCED]->(whenHarryMetSally),
  (noraE)-[:PRODUCED]->(whenHarryMetSally),
  (noraE)-[:WROTE]->(whenHarryMetSally)
CREATE (thatThingYouDo:Movie {title: 'That-Thing-You-Do', released: 1996,
    tagline: 'There comes a time...'})
CREATE (livT:Person {name: 'Liv Tyler', born: 1977})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Mr. White']}]->(thatThingYouDo),
  (livT)-[:ACTED_IN {roles: ['Faye Dolan']}]->(thatThingYouDo),
  (charlize)-[:ACTED_IN {roles: ['Tina']}]->(thatThingYouDo),
  (tomH)-[:DIRECTED]->(thatThingYouDo)
CREATE (theReplacements:Movie {title: 'The Replacements', released: 2000,
    tagline: 'Pain heals, Chicks dig scars... Glory lasts forever'})
CREATE (brooke:Person {name: 'Brooke Langton', born: 1970})
CREATE (gene:Person {name: 'Gene Hackman', born: 1930})
CREATE (orlando:Person {name: 'Orlando Jones', born: 1968})
CREATE (howard:Person {name: 'Howard Deutch', born: 1950})
CREATE
  (keanu)-[:ACTED_IN {roles: ['Shane Falco']}]->(theReplacements),
  (brooke)-[:ACTED_IN {roles: ['Annabelle Farrell']}]->(theReplacements),
  (gene)-[:ACTED_IN {roles: ['Jimmy McGinty']}]->(theReplacements),
  (orlando)-[:ACTED_IN {roles: ['Clifford Franklin']}]->(theReplacements),
  (howard)-[:DIRECTED]->(theReplacements)
CREATE (rescueDawn:Movie {title: 'RescueDawn', released: 2006,
    tagline: 'The extraordinary true story'})
CREATE (christianB:Person {name: 'Christian Bale', born: 1974})
CREATE (zachG:Person {name: 'Zach Grenier', born: 1954})
CREATE
  (marshallB)-[:ACTED_IN {roles: ['Admiral']}]->(rescueDawn),
  (christianB)-[:ACTED_IN {roles: ['Dieter Dengler']}]->(rescueDawn),
  (zachG)-[:ACTED_IN {roles: ['Squad Leader']}]->(rescueDawn),
  (steveZ)-[:ACTED_IN {roles: ['Duane']}]->(rescueDawn),
  (wernerH)-[:DIRECTED]->(rescueDawn)
CREATE (theBirdcage:Movie {title: 'The-Birdcage', released: 1996, tagline: 'Come-as-you-are'})
CREATE (mikeN:Person {name: 'Mike Nichols', born: 1931})
CREATE
  (robin)-[:ACTED_IN {roles: ['Armand Goldman']}]->(theBirdcage),
  (nathan)-[:ACTED_IN {roles: ['Albert Goldman']}]->(theBirdcage),
  (gene)-[:ACTED_IN {roles: ['Sen. Kevin Keeley']}]->(theBirdcage),
  (mikeN)-[:DIRECTED]->(theBirdcage)
CREATE (unforgiven:Movie {title: 'Unforgiven', released: 1992,
    tagline: 'It\'s a hell of a thing, killing a man'})
CREATE (richardH:Person {name: 'Richard Harris', born: 1930})
CREATE (clintE:Person {name: 'Clint Eastwood', born: 1930})
CREATE
  (richardH)-[:ACTED_IN {roles: ['English Bob']}]->(unforgiven),
  (clintE)-[:ACTED_IN {roles: ['Bill Munny']}]->(unforgiven),
  (gene)-[:ACTED_IN {roles: ['Little Bill Daggett']}]->(unforgiven),
  (clintE)-[:DIRECTED]->(unforgiven)
CREATE (johnnyMnemonic:Movie {title: 'Johnny-Mnemonic', released: 1995,
    tagline: 'The-hottest-data-in-the-coolest-head'})
CREATE (takeshi:Person {name: 'Takeshi Kitano', born: 1947})
CREATE (dina:Person {name: 'Dina Meyer', born: 1968})
CREATE (iceT:Person {name: 'Ice-T', born: 1958})
CREATE (robertL:Person {name: 'Robert Longo', born: 1953})
CREATE
  (keanu)-[:ACTED_IN {roles: ['Johnny Mnemonic']}]->(johnnyMnemonic),
  (takeshi)-[:ACTED_IN {roles: ['Takahashi']}]->(johnnyMnemonic),
  (dina)-[:ACTED_IN {roles: ['Jane']}]->(johnnyMnemonic),
  (iceT)-[:ACTED_IN {roles: ['J-Bone']}]->(johnnyMnemonic),
  (robertL)-[:DIRECTED]->(johnnyMnemonic)
CREATE (cloudAtlas:Movie {title: 'Cloud Atlas', released: 2012, tagline: 'Everything is connected'})
CREATE (halleB:Person {name: 'Halle Berry', born: 1966})
CREATE (jimB:Person {name: 'Jim Broadbent', born: 1949})
CREATE (tomT:Person {name: 'Tom Tykwer', born: 1965})
CREATE (davidMitchell:Person {name: 'David Mitchell', born: 1969})
CREATE (stefanArndt:Person {name: 'Stefan Arndt', born: 1961})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Zachry', 'Dr. Henry Goose', 'Isaac Sachs', 'Dermot Hoggins']}]->(cloudAtlas),
  (hugo)-[:ACTED_IN {roles: ['Bill Smoke', 'Haskell Moore', 'Tadeusz Kesselring', 'Nurse Noakes', 'Boardman Mephi', 'Old Georgie']}]->(cloudAtlas),
  (halleB)-[:ACTED_IN {roles: ['Luisa Rey', 'Jocasta Ayrs', 'Ovid', 'Meronym']}]->(cloudAtlas),
  (jimB)-[:ACTED_IN {roles: ['Vyvyan Ayrs', 'Captain Molyneux', 'Timothy Cavendish']}]->(cloudAtlas),
  (tomT)-[:DIRECTED]->(cloudAtlas),
  (andyW)-[:DIRECTED]->(cloudAtlas),
  (lanaW)-[:DIRECTED]->(cloudAtlas),
  (davidMitchell)-[:WROTE]->(cloudAtlas),
  (stefanArndt)-[:PRODUCED]->(cloudAtlas)
CREATE (theDaVinciCode:Movie {title: 'The Da Vinci Code', released: 2006, tagline: 'Break The Codes'})
CREATE (ianM:Person {name: 'Ian McKellen', born: 1939})
CREATE (audreyT:Person {name: 'Audrey Tautou', born: 1976})
CREATE (paulB:Person {name: 'Paul Bettany', born: 1971})
CREATE (ronH:Person {name: 'Ron Howard', born: 1954})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Dr. Robert Langdon']}]->(theDaVinciCode),
  (ianM)-[:ACTED_IN {roles: ['Sir Leight Teabing']}]->(theDaVinciCode),
  (audreyT)-[:ACTED_IN {roles: ['Sophie Neveu']}]->(theDaVinciCode),
  (paulB)-[:ACTED_IN {roles: ['Silas']}]->(theDaVinciCode),
  (ronH)-[:DIRECTED]->(theDaVinciCode)
CREATE (vforVendetta:Movie {title: 'V for Vendetta', released: 2006, tagline: 'Freedom! Forever!'})
CREATE (natalieP:Person {name: 'Natalie Portman', born: 1981})
CREATE (stephenR:Person {name: 'Stephen Rea', born: 1946})
CREATE (johnH:Person {name: 'John Hurt', born: 1940})
CREATE (benM:Person {name: 'Ben Miles', born: 1967})
CREATE
  (hugo)-[:ACTED_IN {roles: ['V']}]->(vforVendetta),
  (natalieP)-[:ACTED_IN {roles: ['Evey Hammond']}]->(vforVendetta),
  (stephenR)-[:ACTED_IN {roles: ['Eric Finch']}]->(vforVendetta),
  (johnH)-[:ACTED_IN {roles: ['High Chancellor Adam Sutler']}]->(vforVendetta),
  (benM)-[:ACTED_IN {roles: ['Dascomb']}]->(vforVendetta),
  (jamesM)-[:DIRECTED]->(vforVendetta),
  (andyW)-[:PRODUCED]->(vforVendetta),
  (lanaW)-[:PRODUCED]->(vforVendetta),
  (joelS)-[:PRODUCED]->(vforVendetta),
  (andyW)-[:WROTE]->(vforVendetta),
  (lanaW)-[:WROTE]->(vforVendetta)
CREATE (speedRacer:Movie {title: 'Speed Racer', released: 2008, tagline: 'Speed has no limits'})
CREATE (emileH:Person {name: 'Emile Hirsch', born: 1985})
CREATE (johnG:Person {name: 'John Goodman', born: 1960})
CREATE (susanS:Person {name: 'Susan Sarandon', born: 1946})
CREATE (matthewF:Person {name: 'Matthew Fox', born: 1966})
CREATE (christinaR:Person {name: 'Christina Ricci', born: 1980})
CREATE (rain:Person {name: 'Rain', born: 1982})
CREATE
  (emileH)-[:ACTED_IN {roles: ['Speed Racer']}]->(speedRacer),
  (johnG)-[:ACTED_IN {roles: ['Pops']}]->(speedRacer),
  (susanS)-[:ACTED_IN {roles: ['Mom']}]->(speedRacer),
  (matthewF)-[:ACTED_IN {roles: ['Racer X']}]->(speedRacer),
  (christinaR)-[:ACTED_IN {roles: ['Trixie']}]->(speedRacer),
  (rain)-[:ACTED_IN {roles: ['Taejo Togokahn']}]->(speedRacer),
  (benM)-[:ACTED_IN {roles: ['Cass Jones']}]->(speedRacer),
  (andyW)-[:DIRECTED]->(speedRacer),
  (lanaW)-[:DIRECTED]->(speedRacer),
  (andyW)-[:WROTE]->(speedRacer),
  (lanaW)-[:WROTE]->(speedRacer),
  (joelS)-[:PRODUCED]->(speedRacer)
CREATE (ninjaAssassin:Movie {title: 'Ninja Assassin', released: 2009,
    tagline: 'Prepare to enter a secret world of assassins'})
CREATE (naomieH:Person {name: 'Naomie Harris'})
CREATE
  (rain)-[:ACTED_IN {roles: ['Raizo']}]->(ninjaAssassin),
  (naomieH)-[:ACTED_IN {roles: ['Mika Coretti']}]->(ninjaAssassin),
  (rickY)-[:ACTED_IN {roles: ['Takeshi']}]->(ninjaAssassin),
  (benM)-[:ACTED_IN {roles: ['Ryan Maslow']}]->(ninjaAssassin),
  (jamesM)-[:DIRECTED]->(ninjaAssassin),
  (andyW)-[:PRODUCED]->(ninjaAssassin),
  (lanaW)-[:PRODUCED]->(ninjaAssassin),
  (joelS)-[:PRODUCED]->(ninjaAssassin)
CREATE (theGreenMile:Movie {title: 'The Green Mile', released: 1999,
    tagline: 'Walk a mile you\'ll never forget.'})
CREATE (michaelD:Person {name: 'Michael Clarke Duncan', born: 1957})
CREATE (davidM:Person {name: 'David Morse', born: 1953})
CREATE (samR:Person {name: 'Sam Rockwell', born: 1968})
CREATE (garyS:Person {name: 'Gary Sinise', born: 1955})
CREATE (patriciaC:Person {name: 'Patricia Clarkson', born: 1959})
CREATE (frankD:Person {name: 'Frank Darabont', born: 1959})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Paul Edgecomb']}]->(theGreenMile),
  (michaelD)-[:ACTED_IN {roles: ['John Coffey']}]->(theGreenMile),
  (davidM)-[:ACTED_IN {roles: ['Brutus Brutal Howell']}]->(theGreenMile),
  (bonnieH)-[:ACTED_IN {roles: ['Jan Edgecomb']}]->(theGreenMile),
  (jamesC)-[:ACTED_IN {roles: ['Warden Hal Moores']}]->(theGreenMile),
  (samR)-[:ACTED_IN {roles: ['Wild Bill Wharton']}]->(theGreenMile),
  (garyS)-[:ACTED_IN {roles: ['Burt Hammersmith']}]->(theGreenMile),
  (patriciaC)-[:ACTED_IN {roles: ['Melinda Moores']}]->(theGreenMile),
  (frankD)-[:DIRECTED]->(theGreenMile)
CREATE (frostNixon:Movie {title: 'Frost/Nixon', released: 2008,
    tagline: '400 million people were waiting for the truth.'})
CREATE (frankL:Person {name: 'Frank Langella', born: 1938})
CREATE (michaelS:Person {name: 'Michael Sheen', born: 1969})
CREATE (oliverP:Person {name: 'Oliver Platt', born: 1960})
CREATE
  (frankL)-[:ACTED_IN {roles: ['Richard Nixon']}]->(frostNixon),
  (michaelS)-[:ACTED_IN {roles: ['David Frost']}]->(frostNixon),
  (kevinB)-[:ACTED_IN {roles: ['Jack Brennan']}]->(frostNixon),
  (oliverP)-[:ACTED_IN {roles: ['Bob Zelnick']}]->(frostNixon),
  (samR)-[:ACTED_IN {roles: ['James Reston, Jr.']}]->(frostNixon),
  (ronH)-[:DIRECTED]->(frostNixon)
CREATE (hoffa:Movie {title: 'Hoffa', released: 1992, tagline: "He didn't want law. He wanted justice."})
CREATE (dannyD:Person {name: 'Danny DeVito', born: 1944})
CREATE (johnR:Person {name: 'John C. Reilly', born: 1965})
CREATE
  (jackN)-[:ACTED_IN {roles: ['Hoffa']}]->(hoffa),
  (dannyD)-[:ACTED_IN {roles: ['Robert Bobby Ciaro']}]->(hoffa),
  (jTW)-[:ACTED_IN {roles: ['Frank Fitzsimmons']}]->(hoffa),
  (johnR)-[:ACTED_IN {roles: ['Peter Connelly']}]->(hoffa),
  (dannyD)-[:DIRECTED]->(hoffa)
CREATE (apollo13:Movie {title: 'Apollo 13', released: 1995, tagline: 'Houston, we have a problem.'})
CREATE (edH:Person {name: 'Ed Harris', born: 1950})
CREATE (billPax:Person {name: 'Bill Paxton', born: 1955})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Jim Lovell']}]->(apollo13),
  (kevinB)-[:ACTED_IN {roles: ['Jack Swigert']}]->(apollo13),
  (edH)-[:ACTED_IN {roles: ['Gene Kranz']}]->(apollo13),
  (billPax)-[:ACTED_IN {roles: ['Fred Haise']}]->(apollo13),
  (garyS)-[:ACTED_IN {roles: ['Ken Mattingly']}]->(apollo13),
  (ronH)-[:DIRECTED]->(apollo13)
CREATE (twister:Movie {title: 'Twister', released: 1996, tagline: 'Don\'t Breathe. Don\'t Look Back.'})
CREATE (philipH:Person {name: 'Philip Seymour Hoffman', born: 1967})
CREATE (janB:Person {name: 'Jan de Bont', born: 1943})
CREATE
  (billPax)-[:ACTED_IN {roles: ['Bill Harding']}]->(twister),
  (helenH)-[:ACTED_IN {roles: ['Dr. Jo Harding']}]->(twister),
  (zachG)-[:ACTED_IN {roles: ['Eddie']}]->(twister),
  (philipH)-[:ACTED_IN {roles: ['Dustin Davis']}]->(twister),
  (janB)-[:DIRECTED]->(twister)
CREATE (castAway:Movie {title: 'Cast Away', released: 2000,
    tagline: 'At the edge of the world, his journey begins.'})
CREATE (robertZ:Person {name: 'Robert Zemeckis', born: 1951})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Chuck Noland']}]->(castAway),
  (helenH)-[:ACTED_IN {roles: ['Kelly Frears']}]->(castAway),
  (robertZ)-[:DIRECTED]->(castAway)
CREATE (oneFlewOvertheCuckoosNest:Movie {title: 'One Flew Over the Cuckoo\'s Nest', released: 1975,
    tagline: 'If he is crazy, what does that make you?'})
CREATE (milosF:Person {name: 'Milos Forman', born: 1932})
CREATE
  (jackN)-[:ACTED_IN {roles: ['Randle McMurphy']}]->(oneFlewOvertheCuckoosNest),
  (dannyD)-[:ACTED_IN {roles: ['Martini']}]->(oneFlewOvertheCuckoosNest),
  (milosF)-[:DIRECTED]->(oneFlewOvertheCuckoosNest)
CREATE (somethingsGottaGive:Movie {title: 'Something\'s Gotta Give', released: 2003})
CREATE (dianeK:Person {name: 'Diane Keaton', born: 1946})
CREATE (nancyM:Person {name: 'Nancy Meyers', born: 1949})
CREATE
  (jackN)-[:ACTED_IN {roles: ['Harry Sanborn']}]->(somethingsGottaGive),
  (dianeK)-[:ACTED_IN {roles: ['Erica Barry']}]->(somethingsGottaGive),
  (keanu)-[:ACTED_IN {roles: ['Julian Mercer']}]->(somethingsGottaGive),
  (nancyM)-[:DIRECTED]->(somethingsGottaGive),
  (nancyM)-[:PRODUCED]->(somethingsGottaGive),
  (nancyM)-[:WROTE]->(somethingsGottaGive)
CREATE (bicentennialMan:Movie {title: 'Bicentennial Man', released: 1999,
    tagline: 'One robot\'s 200 year journey to become an ordinary man.'})
CREATE (chrisC:Person {name: 'Chris Columbus', born: 1958})
CREATE
  (robin)-[:ACTED_IN {roles: ['Andrew Marin']}]->(bicentennialMan),
  (oliverP)-[:ACTED_IN {roles: ['Rupert Burns']}]->(bicentennialMan),
  (chrisC)-[:DIRECTED]->(bicentennialMan)
CREATE (charlieWilsonsWar:Movie {title: 'Charlie Wilson\'s War', released: 2007,
    tagline: 'A stiff drink. A little mascara. A lot of nerve. Who said they could not bring down the Soviet empire.'})
CREATE (juliaR:Person {name: 'Julia Roberts', born: 1967})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Rep. Charlie Wilson']}]->(charlieWilsonsWar),
  (juliaR)-[:ACTED_IN {roles: ['Joanne Herring']}]->(charlieWilsonsWar),
  (philipH)-[:ACTED_IN {roles: ['Gust Avrakotos']}]->(charlieWilsonsWar),
  (mikeN)-[:DIRECTED]->(charlieWilsonsWar)
CREATE (thePolarExpress:Movie {title: 'The Polar Express', released: 2004,
    tagline: 'This Holiday Season... Believe'})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Hero Boy', 'Father', 'Conductor', 'Hobo', 'Scrooge', 'Santa Claus']}]->(thePolarExpress),
  (robertZ)-[:DIRECTED]->(thePolarExpress)
CREATE (aLeagueofTheirOwn:Movie {title: 'A League of Their Own', released: 1992,
    tagline: 'A league of their own'})
CREATE (madonna:Person {name: 'Madonna', born: 1954})
CREATE (geenaD:Person {name: 'Geena Davis', born: 1956})
CREATE (loriP:Person {name: 'Lori Petty', born: 1963})
CREATE (pennyM:Person {name: 'Penny Marshall', born: 1943})
CREATE
  (tomH)-[:ACTED_IN {roles: ['Jimmy Dugan']}]->(aLeagueofTheirOwn),
  (geenaD)-[:ACTED_IN {roles: ['Dottie Hinson']}]->(aLeagueofTheirOwn),
  (loriP)-[:ACTED_IN {roles: ['Kit Keller']}]->(aLeagueofTheirOwn),
  (rosieO)-[:ACTED_IN {roles: ['Doris Murphy']}]->(aLeagueofTheirOwn),
  (madonna)-[:ACTED_IN {roles: ['Mae Mordabito']}]->(aLeagueofTheirOwn),
  (billPax)-[:ACTED_IN {roles: ['Bob Hinson']}]->(aLeagueofTheirOwn),
  (pennyM)-[:DIRECTED]->(aLeagueofTheirOwn)
CREATE (paulBlythe:Person {name: 'Paul Blythe'})
CREATE (angelaScope:Person {name: 'Angela Scope'})
CREATE (jessicaThompson:Person {name: 'Jessica Thompson'})
CREATE (jamesThompson:Person {name: 'James Thompson'})
CREATE
  (jamesThompson)-[:FOLLOWS]->(jessicaThompson),
  (angelaScope)-[:FOLLOWS]->(jessicaThompson),
  (paulBlythe)-[:FOLLOWS]->(angelaScope)
CREATE
  (jessicaThompson)-[:REVIEWED {summary: 'An amazing journey', rating: 95}]->(cloudAtlas),
  (jessicaThompson)-[:REVIEWED {summary: 'Silly, but fun', rating: 65}]->(theReplacements),
  (jamesThompson)-[:REVIEWED {summary: 'The coolest football movie ever', rating: 100}]->(theReplacements),
  (angelaScope)-[:REVIEWED {summary: 'Pretty funny at times', rating: 62}]->(theReplacements),
  (jessicaThompson)-[:REVIEWED {summary: 'Dark, but compelling', rating: 85}]->(unforgiven),
  (jessicaThompson)-[:REVIEWED {summary: 'Slapstick', rating: 45}]->(theBirdcage),
  (jessicaThompson)-[:REVIEWED {summary: 'A solid romp', rating: 68}]->(theDaVinciCode),
  (jamesThompson)-[:REVIEWED {summary: 'Fun, but a little far fetched', rating: 65}]->(theDaVinciCode),
  (jessicaThompson)-[:REVIEWED {summary: 'You had me at Jerry', rating: 92}]->(jerryMaguire)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create4.feature
CREATE (hf:School {name: 'Hilly Fields Technical College'})
CREATE (hf)-[:STAFF]->(mrb:Teacher {name: 'Mr Balls'})
CREATE (hf)-[:STAFF]->(mrspb:Teacher {name: 'Ms Packard-Bell'})
CREATE (hf)-[:STAFF]->(mrs:Teacher {name: 'Mr Smith'})
CREATE (hf)-[:STAFF]->(mrsa:Teacher {name: 'Mrs Adenough'})
CREATE (hf)-[:STAFF]->(mrvdg:Teacher {name: 'Mr Van der Graaf'})
CREATE (hf)-[:STAFF]->(msn:Teacher {name: 'Ms Noethe'})
CREATE (hf)-[:STAFF]->(mrsn:Teacher {name: 'Mrs Noakes'})
CREATE (hf)-[:STAFF]->(mrm:Teacher {name: 'Mr Marker'})
CREATE (hf)-[:STAFF]->(msd:Teacher {name: 'Ms Delgado'})
CREATE (hf)-[:STAFF]->(mrsg:Teacher {name: 'Mrs Glass'})
CREATE (hf)-[:STAFF]->(mrf:Teacher {name: 'Mr Flint'})
CREATE (hf)-[:STAFF]->(mrk:Teacher {name: 'Mr Kearney'})
CREATE (hf)-[:STAFF]->(msf:Teacher {name: 'Mrs Forrester'})
CREATE (hf)-[:STAFF]->(mrsf:Teacher {name: 'Mrs Fischer'})
CREATE (hf)-[:STAFF]->(mrj:Teacher {name: 'Mr Jameson'})
CREATE (hf)-[:STUDENT]->(_001:Student {name: 'Portia Vasquez'})
CREATE (hf)-[:STUDENT]->(_002:Student {name: 'Andrew Parks'})
CREATE (hf)-[:STUDENT]->(_003:Student {name: 'Germane Frye'})
CREATE (hf)-[:STUDENT]->(_004:Student {name: 'Yuli Gutierrez'})
CREATE (hf)-[:STUDENT]->(_005:Student {name: 'Kamal Solomon'})
CREATE (hf)-[:STUDENT]->(_006:Student {name: 'Lysandra Porter'})
CREATE (hf)-[:STUDENT]->(_007:Student {name: 'Stella Santiago'})
CREATE (hf)-[:STUDENT]->(_008:Student {name: 'Brenda Torres'})
CREATE (hf)-[:STUDENT]->(_009:Student {name: 'Heidi Dunlap'})
CREATE (hf)-[:STUDENT]->(_010:Student {name: 'Halee Taylor'})
CREATE (hf)-[:STUDENT]->(_011:Student {name: 'Brennan Crosby'})
CREATE (hf)-[:STUDENT]->(_012:Student {name: 'Rooney Cook'})
CREATE (hf)-[:STUDENT]->(_013:Student {name: 'Xavier Morrison'})
CREATE (hf)-[:STUDENT]->(_014:Student {name: 'Zelenia Santana'})
CREATE (hf)-[:STUDENT]->(_015:Student {name: 'Eaton Bonner'})
CREATE (hf)-[:STUDENT]->(_016:Student {name: 'Leilani Bishop'})
CREATE (hf)-[:STUDENT]->(_017:Student {name: 'Jamalia Pickett'})
CREATE (hf)-[:STUDENT]->(_018:Student {name: 'Wynter Russell'})
CREATE (hf)-[:STUDENT]->(_019:Student {name: 'Liberty Melton'})
CREATE (hf)-[:STUDENT]->(_020:Student {name: 'MacKensie Obrien'})
CREATE (hf)-[:STUDENT]->(_021:Student {name: 'Oprah Maynard'})
CREATE (hf)-[:STUDENT]->(_022:Student {name: 'Lyle Parks'})
CREATE (hf)-[:STUDENT]->(_023:Student {name: 'Madonna Justice'})
CREATE (hf)-[:STUDENT]->(_024:Student {name: 'Herman Frederick'})
CREATE (hf)-[:STUDENT]->(_025:Student {name: 'Preston Stevenson'})
CREATE (hf)-[:STUDENT]->(_026:Student {name: 'Drew Carrillo'})
CREATE (hf)-[:STUDENT]->(_027:Student {name: 'Hamilton Woodward'})
CREATE (hf)-[:STUDENT]->(_028:Student {name: 'Buckminster Bradley'})
CREATE (hf)-[:STUDENT]->(_029:Student {name: 'Shea Cote'})
CREATE (hf)-[:STUDENT]->(_030:Student {name: 'Raymond Leonard'})
CREATE (hf)-[:STUDENT]->(_031:Student {name: 'Gavin Branch'})
CREATE (hf)-[:STUDENT]->(_032:Student {name: 'Kylan Powers'})
CREATE (hf)-[:STUDENT]->(_033:Student {name: 'Hedy Bowers'})
CREATE (hf)-[:STUDENT]->(_034:Student {name: 'Derek Church'})
CREATE (hf)-[:STUDENT]->(_035:Student {name: 'Silas Santiago'})
CREATE (hf)-[:STUDENT]->(_036:Student {name: 'Elton Bright'})
CREATE (hf)-[:STUDENT]->(_037:Student {name: 'Dora Schmidt'})
CREATE (hf)-[:STUDENT]->(_038:Student {name: 'Julian Sullivan'})
CREATE (hf)-[:STUDENT]->(_039:Student {name: 'Willow Morton'})
CREATE (hf)-[:STUDENT]->(_040:Student {name: 'Blaze Hines'})
CREATE (hf)-[:STUDENT]->(_041:Student {name: 'Felicia Tillman'})
CREATE (hf)-[:STUDENT]->(_042:Student {name: 'Ralph Webb'})
CREATE (hf)-[:STUDENT]->(_043:Student {name: 'Roth Gilmore'})
CREATE (hf)-[:STUDENT]->(_044:Student {name: 'Dorothy Burgess'})
CREATE (hf)-[:STUDENT]->(_045:Student {name: 'Lana Sandoval'})
CREATE (hf)-[:STUDENT]->(_046:Student {name: 'Nevada Strickland'})
CREATE (hf)-[:STUDENT]->(_047:Student {name: 'Lucian Franco'})
CREATE (hf)-[:STUDENT]->(_048:Student {name: 'Jasper Talley'})
CREATE (hf)-[:STUDENT]->(_049:Student {name: 'Madaline Spears'})
CREATE (hf)-[:STUDENT]->(_050:Student {name: 'Upton Browning'})
CREATE (hf)-[:STUDENT]->(_051:Student {name: 'Cooper Leon'})
CREATE (hf)-[:STUDENT]->(_052:Student {name: 'Celeste Ortega'})
CREATE (hf)-[:STUDENT]->(_053:Student {name: 'Willa Hewitt'})
CREATE (hf)-[:STUDENT]->(_054:Student {name: 'Rooney Bryan'})
CREATE (hf)-[:STUDENT]->(_055:Student {name: 'Nayda Hays'})
CREATE (hf)-[:STUDENT]->(_056:Student {name: 'Kadeem Salazar'})
CREATE (hf)-[:STUDENT]->(_057:Student {name: 'Halee Allen'})
CREATE (hf)-[:STUDENT]->(_058:Student {name: 'Odysseus Mayo'})
CREATE (hf)-[:STUDENT]->(_059:Student {name: 'Kato Merrill'})
CREATE (hf)-[:STUDENT]->(_060:Student {name: 'Halee Juarez'})
CREATE (hf)-[:STUDENT]->(_061:Student {name: 'Chloe Charles'})
CREATE (hf)-[:STUDENT]->(_062:Student {name: 'Abel Montoya'})
CREATE (hf)-[:STUDENT]->(_063:Student {name: 'Hilda Welch'})
CREATE (hf)-[:STUDENT]->(_064:Student {name: 'Britanni Bean'})
CREATE (hf)-[:STUDENT]->(_065:Student {name: 'Joelle Beach'})
CREATE (hf)-[:STUDENT]->(_066:Student {name: 'Ciara Odom'})
CREATE (hf)-[:STUDENT]->(_067:Student {name: 'Zia Williams'})
CREATE (hf)-[:STUDENT]->(_068:Student {name: 'Darrel Bailey'})
CREATE (hf)-[:STUDENT]->(_069:Student {name: 'Lance Mcdowell'})
CREATE (hf)-[:STUDENT]->(_070:Student {name: 'Clayton Bullock'})
CREATE (hf)-[:STUDENT]->(_071:Student {name: 'Roanna Mosley'})
CREATE (hf)-[:STUDENT]->(_072:Student {name: 'Amethyst Mcclure'})
CREATE (hf)-[:STUDENT]->(_073:Student {name: 'Hanae Mann'})
CREATE (hf)-[:STUDENT]->(_074:Student {name: 'Graiden Haynes'})
CREATE (hf)-[:STUDENT]->(_075:Student {name: 'Marcia Byrd'})
CREATE (hf)-[:STUDENT]->(_076:Student {name: 'Yoshi Joyce'})
CREATE (hf)-[:STUDENT]->(_077:Student {name: 'Gregory Sexton'})
CREATE (hf)-[:STUDENT]->(_078:Student {name: 'Nash Carey'})
CREATE (hf)-[:STUDENT]->(_079:Student {name: 'Rae Stevens'})
CREATE (hf)-[:STUDENT]->(_080:Student {name: 'Blossom Fulton'})
CREATE (hf)-[:STUDENT]->(_081:Student {name: 'Lev Curry'})
CREATE (hf)-[:STUDENT]->(_082:Student {name: 'Margaret Gamble'})
CREATE (hf)-[:STUDENT]->(_083:Student {name: 'Rylee Patterson'})
CREATE (hf)-[:STUDENT]->(_084:Student {name: 'Harper Perkins'})
CREATE (hf)-[:STUDENT]->(_085:Student {name: 'Kennan Murphy'})
CREATE (hf)-[:STUDENT]->(_086:Student {name: 'Hilda Coffey'})
CREATE (hf)-[:STUDENT]->(_087:Student {name: 'Marah Reed'})
CREATE (hf)-[:STUDENT]->(_088:Student {name: 'Blaine Wade'})
CREATE (hf)-[:STUDENT]->(_089:Student {name: 'Geraldine Sanders'})
CREATE (hf)-[:STUDENT]->(_090:Student {name: 'Kerry Rollins'})
CREATE (hf)-[:STUDENT]->(_091:Student {name: 'Virginia Sweet'})
CREATE (hf)-[:STUDENT]->(_092:Student {name: 'Sophia Merrill'})
CREATE (hf)-[:STUDENT]->(_093:Student {name: 'Hedda Carson'})
CREATE (hf)-[:STUDENT]->(_094:Student {name: 'Tamekah Charles'})
CREATE (hf)-[:STUDENT]->(_095:Student {name: 'Knox Barton'})
CREATE (hf)-[:STUDENT]->(_096:Student {name: 'Ariel Porter'})
CREATE (hf)-[:STUDENT]->(_097:Student {name: 'Berk Wooten'})
CREATE (hf)-[:STUDENT]->(_098:Student {name: 'Galena Glenn'})
CREATE (hf)-[:STUDENT]->(_099:Student {name: 'Jolene Anderson'})
CREATE (hf)-[:STUDENT]->(_100:Student {name: 'Leonard Hewitt'})
CREATE (hf)-[:STUDENT]->(_101:Student {name: 'Maris Salazar'})
CREATE (hf)-[:STUDENT]->(_102:Student {name: 'Brian Frost'})
CREATE (hf)-[:STUDENT]->(_103:Student {name: 'Zane Moses'})
CREATE (hf)-[:STUDENT]->(_104:Student {name: 'Serina Finch'})
CREATE (hf)-[:STUDENT]->(_105:Student {name: 'Anastasia Fletcher'})
CREATE (hf)-[:STUDENT]->(_106:Student {name: 'Glenna Chapman'})
CREATE (hf)-[:STUDENT]->(_107:Student {name: 'Mufutau Gillespie'})
CREATE (hf)-[:STUDENT]->(_108:Student {name: 'Basil Guthrie'})
CREATE (hf)-[:STUDENT]->(_109:Student {name: 'Theodore Marsh'})
CREATE (hf)-[:STUDENT]->(_110:Student {name: 'Jaime Contreras'})
CREATE (hf)-[:STUDENT]->(_111:Student {name: 'Irma Poole'})
CREATE (hf)-[:STUDENT]->(_112:Student {name: 'Buckminster Bender'})
CREATE (hf)-[:STUDENT]->(_113:Student {name: 'Elton Morris'})
CREATE (hf)-[:STUDENT]->(_114:Student {name: 'Barbara Nguyen'})
CREATE (hf)-[:STUDENT]->(_115:Student {name: 'Tanya Kidd'})
CREATE (hf)-[:STUDENT]->(_116:Student {name: 'Kaden Hoover'})
CREATE (hf)-[:STUDENT]->(_117:Student {name: 'Christopher Bean'})
CREATE (hf)-[:STUDENT]->(_118:Student {name: 'Trevor Daugherty'})
CREATE (hf)-[:STUDENT]->(_119:Student {name: 'Rudyard Bates'})
CREATE (hf)-[:STUDENT]->(_120:Student {name: 'Stacy Monroe'})
CREATE (hf)-[:STUDENT]->(_121:Student {name: 'Kieran Keller'})
CREATE (hf)-[:STUDENT]->(_122:Student {name: 'Ivy Garrison'})
CREATE (hf)-[:STUDENT]->(_123:Student {name: 'Miranda Haynes'})
CREATE (hf)-[:STUDENT]->(_124:Student {name: 'Abigail Heath'})
CREATE (hf)-[:STUDENT]->(_125:Student {name: 'Margaret Santiago'})
CREATE (hf)-[:STUDENT]->(_126:Student {name: 'Cade Floyd'})
CREATE (hf)-[:STUDENT]->(_127:Student {name: 'Allen Crane'})
CREATE (hf)-[:STUDENT]->(_128:Student {name: 'Stella Gilliam'})
CREATE (hf)-[:STUDENT]->(_129:Student {name: 'Rashad Miller'})
CREATE (hf)-[:STUDENT]->(_130:Student {name: 'Francis Cox'})
CREATE (hf)-[:STUDENT]->(_131:Student {name: 'Darryl Rosario'})
CREATE (hf)-[:STUDENT]->(_132:Student {name: 'Michael Daniels'})
CREATE (hf)-[:STUDENT]->(_133:Student {name: 'Aretha Henderson'})
CREATE (hf)-[:STUDENT]->(_134:Student {name: 'Roth Barrera'})
CREATE (hf)-[:STUDENT]->(_135:Student {name: 'Yael Day'})
CREATE (hf)-[:STUDENT]->(_136:Student {name: 'Wynter Richmond'})
CREATE (hf)-[:STUDENT]->(_137:Student {name: 'Quyn Flowers'})
CREATE (hf)-[:STUDENT]->(_138:Student {name: 'Yvette Marquez'})
CREATE (hf)-[:STUDENT]->(_139:Student {name: 'Teagan Curry'})
CREATE (hf)-[:STUDENT]->(_140:Student {name: 'Brenden Bishop'})
CREATE (hf)-[:STUDENT]->(_141:Student {name: 'Montana Black'})
CREATE (hf)-[:STUDENT]->(_142:Student {name: 'Ramona Parker'})
CREATE (hf)-[:STUDENT]->(_143:Student {name: 'Merritt Hansen'})
CREATE (hf)-[:STUDENT]->(_144:Student {name: 'Melvin Vang'})
CREATE (hf)-[:STUDENT]->(_145:Student {name: 'Samantha Perez'})
CREATE (hf)-[:STUDENT]->(_146:Student {name: 'Thane Porter'})
CREATE (hf)-[:STUDENT]->(_147:Student {name: 'Vaughan Haynes'})
CREATE (hf)-[:STUDENT]->(_148:Student {name: 'Irma Miles'})
CREATE (hf)-[:STUDENT]->(_149:Student {name: 'Amery Jensen'})
CREATE (hf)-[:STUDENT]->(_150:Student {name: 'Montana Holman'})
CREATE (hf)-[:STUDENT]->(_151:Student {name: 'Kimberly Langley'})
CREATE (hf)-[:STUDENT]->(_152:Student {name: 'Ebony Bray'})
CREATE (hf)-[:STUDENT]->(_153:Student {name: 'Ishmael Pollard'})
CREATE (hf)-[:STUDENT]->(_154:Student {name: 'Illana Thompson'})
CREATE (hf)-[:STUDENT]->(_155:Student {name: 'Rhona Bowers'})
CREATE (hf)-[:STUDENT]->(_156:Student {name: 'Lilah Dotson'})
CREATE (hf)-[:STUDENT]->(_157:Student {name: 'Shelly Roach'})
CREATE (hf)-[:STUDENT]->(_158:Student {name: 'Celeste Woodward'})
CREATE (hf)-[:STUDENT]->(_159:Student {name: 'Christen Lynn'})
CREATE (hf)-[:STUDENT]->(_160:Student {name: 'Miranda Slater'})
CREATE (hf)-[:STUDENT]->(_161:Student {name: 'Lunea Clements'})
CREATE (hf)-[:STUDENT]->(_162:Student {name: 'Lester Francis'})
CREATE (hf)-[:STUDENT]->(_163:Student {name: 'David Fischer'})
CREATE (hf)-[:STUDENT]->(_164:Student {name: 'Kyra Bean'})
CREATE (hf)-[:STUDENT]->(_165:Student {name: 'Imelda Alston'})
CREATE (hf)-[:STUDENT]->(_166:Student {name: 'Finn Farrell'})
CREATE (hf)-[:STUDENT]->(_167:Student {name: 'Kirby House'})
CREATE (hf)-[:STUDENT]->(_168:Student {name: 'Amanda Zamora'})
CREATE (hf)-[:STUDENT]->(_169:Student {name: 'Rina Franco'})
CREATE (hf)-[:STUDENT]->(_170:Student {name: 'Sonia Lane'})
CREATE (hf)-[:STUDENT]->(_171:Student {name: 'Nora Jefferson'})
CREATE (hf)-[:STUDENT]->(_172:Student {name: 'Colton Ortiz'})
CREATE (hf)-[:STUDENT]->(_173:Student {name: 'Alden Munoz'})
CREATE (hf)-[:STUDENT]->(_174:Student {name: 'Ferdinand Cline'})
CREATE (hf)-[:STUDENT]->(_175:Student {name: 'Cynthia Prince'})
CREATE (hf)-[:STUDENT]->(_176:Student {name: 'Asher Hurst'})
CREATE (hf)-[:STUDENT]->(_177:Student {name: 'MacKensie Stevenson'})
CREATE (hf)-[:STUDENT]->(_178:Student {name: 'Sydnee Sosa'})
CREATE (hf)-[:STUDENT]->(_179:Student {name: 'Dante Callahan'})
CREATE (hf)-[:STUDENT]->(_180:Student {name: 'Isabella Santana'})
CREATE (hf)-[:STUDENT]->(_181:Student {name: 'Raven Bowman'})
CREATE (hf)-[:STUDENT]->(_182:Student {name: 'Kirby Bolton'})
CREATE (hf)-[:STUDENT]->(_183:Student {name: 'Peter Shaffer'})
CREATE (hf)-[:STUDENT]->(_184:Student {name: 'Fletcher Beard'})
CREATE (hf)-[:STUDENT]->(_185:Student {name: 'Irene Lowe'})
CREATE (hf)-[:STUDENT]->(_186:Student {name: 'Ella Talley'})
CREATE (hf)-[:STUDENT]->(_187:Student {name: 'Jorden Kerr'})
CREATE (hf)-[:STUDENT]->(_188:Student {name: 'Macey Delgado'})
CREATE (hf)-[:STUDENT]->(_189:Student {name: 'Ulysses Graves'})
CREATE (hf)-[:STUDENT]->(_190:Student {name: 'Declan Blake'})
CREATE (hf)-[:STUDENT]->(_191:Student {name: 'Lila Hurst'})
CREATE (hf)-[:STUDENT]->(_192:Student {name: 'David Rasmussen'})
CREATE (hf)-[:STUDENT]->(_193:Student {name: 'Desiree Cortez'})
CREATE (hf)-[:STUDENT]->(_194:Student {name: 'Myles Horton'})
CREATE (hf)-[:STUDENT]->(_195:Student {name: 'Rylee Willis'})
CREATE (hf)-[:STUDENT]->(_196:Student {name: 'Kelsey Yates'})
CREATE (hf)-[:STUDENT]->(_197:Student {name: 'Alika Stanton'})
CREATE (hf)-[:STUDENT]->(_198:Student {name: 'Ria Campos'})
CREATE (hf)-[:STUDENT]->(_199:Student {name: 'Elijah Hendricks'})
CREATE (hf)-[:STUDENT]->(_200:Student {name: 'Hayes House'})
CREATE (hf)-[:DEPARTMENT]->(md:Department {name: 'Mathematics'})
CREATE (hf)-[:DEPARTMENT]->(sd:Department {name: 'Science'})
CREATE (hf)-[:DEPARTMENT]->(ed:Department {name: 'Engineering'})
CREATE (pm:Subject {name: 'Pure Mathematics'})
CREATE (am:Subject {name: 'Applied Mathematics'})
CREATE (ph:Subject {name: 'Physics'})
CREATE (ch:Subject {name: 'Chemistry'})
CREATE (bi:Subject {name: 'Biology'})
CREATE (es:Subject {name: 'Earth Science'})
CREATE (me:Subject {name: 'Mechanical Engineering'})
CREATE (ce:Subject {name: 'Chemical Engineering'})
CREATE (se:Subject {name: 'Systems Engineering'})
CREATE (ve:Subject {name: 'Civil Engineering'})
CREATE (ee:Subject {name: 'Electrical Engineering'})
CREATE (sd)-[:CURRICULUM]->(ph)
CREATE (sd)-[:CURRICULUM]->(ch)
CREATE (sd)-[:CURRICULUM]->(bi)
CREATE (sd)-[:CURRICULUM]->(es)
CREATE (md)-[:CURRICULUM]->(pm)
CREATE (md)-[:CURRICULUM]->(am)
CREATE (ed)-[:CURRICULUM]->(me)
CREATE (ed)-[:CURRICULUM]->(se)
CREATE (ed)-[:CURRICULUM]->(ce)
CREATE (ed)-[:CURRICULUM]->(ee)
CREATE (ed)-[:CURRICULUM]->(ve)
CREATE (ph)-[:TAUGHT_BY]->(mrb)
CREATE (ph)-[:TAUGHT_BY]->(mrk)
CREATE (ch)-[:TAUGHT_BY]->(mrk)
CREATE (ch)-[:TAUGHT_BY]->(mrsn)
CREATE (bi)-[:TAUGHT_BY]->(mrsn)
CREATE (bi)-[:TAUGHT_BY]->(mrsf)
CREATE (es)-[:TAUGHT_BY]->(msn)
CREATE (pm)-[:TAUGHT_BY]->(mrf)
CREATE (pm)-[:TAUGHT_BY]->(mrm)
CREATE (pm)-[:TAUGHT_BY]->(mrvdg)
CREATE (am)-[:TAUGHT_BY]->(mrsg)
CREATE (am)-[:TAUGHT_BY]->(mrspb)
CREATE (am)-[:TAUGHT_BY]->(mrvdg)
CREATE (me)-[:TAUGHT_BY]->(mrj)
CREATE (ce)-[:TAUGHT_BY]->(mrsa)
CREATE (se)-[:TAUGHT_BY]->(mrs)
CREATE (ve)-[:TAUGHT_BY]->(msd)
CREATE (ee)-[:TAUGHT_BY]->(mrsf)
CREATE(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
CREATE(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
CREATE(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_106)
CREATE(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
CREATE(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_153)
CREATE(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
CREATE(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
CREATE(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
CREATE(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
CREATE(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
CREATE(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_043)
CREATE(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
CREATE(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
CREATE(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_165)
CREATE(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
CREATE(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
CREATE(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_062)
CREATE(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_033)
CREATE(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
CREATE(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
CREATE(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
CREATE(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_121)
CREATE(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_049)
CREATE(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
CREATE(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
CREATE(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_085)
CREATE(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
CREATE(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
CREATE(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
CREATE(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
CREATE(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
CREATE(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
CREATE(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
CREATE(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
CREATE(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
CREATE(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
CREATE(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_049)
CREATE(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
CREATE(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
CREATE(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_149)
CREATE(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
CREATE(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
CREATE(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
CREATE(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
CREATE(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
CREATE(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_189)
CREATE(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
CREATE(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
CREATE(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
CREATE(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
CREATE(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_148)
CREATE(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
CREATE(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
CREATE(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
CREATE(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_042)
CREATE(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
CREATE(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
CREATE(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_034)
CREATE(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_088)
CREATE(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
CREATE(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_88)
CREATE(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
CREATE(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
CREATE(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
CREATE(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
CREATE(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
CREATE(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
CREATE(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
CREATE(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
CREATE(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
CREATE(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_128)
CREATE(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_149)
CREATE(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
CREATE(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_104)
CREATE(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
CREATE(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
CREATE(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
CREATE(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
CREATE(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
CREATE(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
CREATE(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
CREATE(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
CREATE(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
CREATE(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_128)
CREATE(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
CREATE(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
CREATE(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_137)
CREATE(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
CREATE(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_156)
CREATE(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
CREATE(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
CREATE(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
CREATE(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_006)
CREATE(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
CREATE(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
CREATE(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_001)
CREATE(_101)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
CREATE(_102)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
CREATE(_103)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_136)
CREATE(_104)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
CREATE(_105)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_127)
CREATE(_106)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
CREATE(_107)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_036)
CREATE(_108)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
CREATE(_109)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
CREATE(_110)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
CREATE(_111)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
CREATE(_112)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
CREATE(_113)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_035)
CREATE(_114)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
CREATE(_115)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_070)
CREATE(_116)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
CREATE(_117)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
CREATE(_118)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
CREATE(_119)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
CREATE(_120)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_167)
CREATE(_121)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
CREATE(_122)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
CREATE(_123)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_004)
CREATE(_124)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
CREATE(_125)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
CREATE(_126)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
CREATE(_127)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_168)
CREATE(_128)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
CREATE(_129)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
CREATE(_130)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
CREATE(_131)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
CREATE(_132)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
CREATE(_133)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
CREATE(_134)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_037)
CREATE(_135)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_012)
CREATE(_136)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
CREATE(_137)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
CREATE(_138)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
CREATE(_139)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
CREATE(_140)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_170)
CREATE(_141)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
CREATE(_142)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_139)
CREATE(_143)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
CREATE(_144)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
CREATE(_145)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
CREATE(_146)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
CREATE(_147)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
CREATE(_148)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
CREATE(_149)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_155)
CREATE(_150)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_151)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
CREATE(_152)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
CREATE(_153)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_179)
CREATE(_154)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
CREATE(_155)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_058)
CREATE(_156)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_071)
CREATE(_157)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
CREATE(_158)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
CREATE(_159)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
CREATE(_160)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
CREATE(_161)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
CREATE(_162)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
CREATE(_163)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_163)
CREATE(_164)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
CREATE(_165)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
CREATE(_166)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_136)
CREATE(_167)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
CREATE(_168)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_110)
CREATE(_169)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
CREATE(_170)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
CREATE(_171)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
CREATE(_172)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
CREATE(_173)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
CREATE(_174)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
CREATE(_175)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
CREATE(_176)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
CREATE(_177)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
CREATE(_178)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_006)
CREATE(_179)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
CREATE(_180)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_185)
CREATE(_181)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
CREATE(_182)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
CREATE(_183)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
CREATE(_184)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
CREATE(_185)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
CREATE(_186)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
CREATE(_187)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
CREATE(_188)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
CREATE(_189)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
CREATE(_190)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_063)
CREATE(_191)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_079)
CREATE(_192)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_121)
CREATE(_193)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
CREATE(_194)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
CREATE(_195)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
CREATE(_196)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
CREATE(_197)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
CREATE(_198)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_039)
CREATE(_199)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
CREATE(_200)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
CREATE(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
CREATE(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_078)
CREATE(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
CREATE(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
CREATE(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
CREATE(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
CREATE(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
CREATE(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
CREATE(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_147)
CREATE(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
CREATE(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_118)
CREATE(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
CREATE(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
CREATE(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
CREATE(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
CREATE(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
CREATE(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
CREATE(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
CREATE(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
CREATE(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
CREATE(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
CREATE(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
CREATE(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_087)
CREATE(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_163)
CREATE(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
CREATE(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_042)
CREATE(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_058)
CREATE(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
CREATE(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
CREATE(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
CREATE(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
CREATE(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_158)
CREATE(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
CREATE(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_102)
CREATE(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
CREATE(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
CREATE(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
CREATE(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
CREATE(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
CREATE(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_085)
CREATE(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_025)
CREATE(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
CREATE(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_118)
CREATE(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
CREATE(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_071)
CREATE(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
CREATE(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
CREATE(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
CREATE(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
CREATE(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_185)
CREATE(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
CREATE(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
CREATE(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
CREATE(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
CREATE(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
CREATE(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
CREATE(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
CREATE(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
CREATE(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
CREATE(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
CREATE(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
CREATE(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_153)
CREATE(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
CREATE(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
CREATE(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
CREATE(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
CREATE(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
CREATE(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
CREATE(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
CREATE(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
CREATE(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
CREATE(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_179)
CREATE(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_005)
CREATE(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
CREATE(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_017)
CREATE(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
CREATE(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
CREATE(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
CREATE(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
CREATE(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
CREATE(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
CREATE(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
CREATE(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
CREATE(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
CREATE(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_064)
CREATE(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
CREATE(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_077)
CREATE(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_124)
CREATE(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
CREATE(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
CREATE(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
CREATE(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_093)
CREATE(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
CREATE(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
CREATE(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
CREATE(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_043)
CREATE(_101)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
CREATE(_102)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
CREATE(_103)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_040)
CREATE(_104)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
CREATE(_105)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_063)
CREATE(_106)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_180)
CREATE(_107)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
CREATE(_108)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
CREATE(_109)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
CREATE(_110)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
CREATE(_111)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
CREATE(_112)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
CREATE(_113)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
CREATE(_114)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
CREATE(_115)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_028)
CREATE(_116)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
CREATE(_117)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
CREATE(_118)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
CREATE(_119)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
CREATE(_120)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
CREATE(_121)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
CREATE(_122)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_127)
CREATE(_123)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
CREATE(_124)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
CREATE(_125)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
CREATE(_126)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
CREATE(_127)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
CREATE(_128)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_165)
CREATE(_129)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
CREATE(_130)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_066)
CREATE(_131)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
CREATE(_132)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
CREATE(_133)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
CREATE(_134)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
CREATE(_135)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
CREATE(_136)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
CREATE(_137)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
CREATE(_138)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
CREATE(_139)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
CREATE(_140)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_013)
CREATE(_141)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
CREATE(_142)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
CREATE(_143)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
CREATE(_144)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_145)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
CREATE(_146)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
CREATE(_147)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_009)
CREATE(_148)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
CREATE(_149)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
CREATE(_150)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
CREATE(_151)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_055)
CREATE(_152)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_157)
CREATE(_153)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
CREATE(_154)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
CREATE(_155)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
CREATE(_156)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
CREATE(_157)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
CREATE(_158)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_154)
CREATE(_159)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_160)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_168)
CREATE(_161)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
CREATE(_162)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
CREATE(_163)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
CREATE(_164)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_087)
CREATE(_165)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_104)
CREATE(_166)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
CREATE(_167)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
CREATE(_168)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
CREATE(_169)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
CREATE(_170)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_183)
CREATE(_171)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_147)
CREATE(_172)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
CREATE(_173)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
CREATE(_174)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_137)
CREATE(_175)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
CREATE(_176)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
CREATE(_177)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_078)
CREATE(_178)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
CREATE(_179)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_062)
CREATE(_180)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
CREATE(_181)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
CREATE(_182)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
CREATE(_183)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
CREATE(_184)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
CREATE(_185)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
CREATE(_186)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
CREATE(_187)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
CREATE(_188)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
CREATE(_189)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_093)
CREATE(_190)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_191)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_183)
CREATE(_192)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
CREATE(_193)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
CREATE(_194)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
CREATE(_195)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
CREATE(_196)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
CREATE(_197)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
CREATE(_198)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_190)
CREATE(_199)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
CREATE(_200)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
CREATE(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
CREATE(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
CREATE(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
CREATE(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
CREATE(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
CREATE(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_066)
CREATE(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
CREATE(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
CREATE(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
CREATE(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
CREATE(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
CREATE(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_036)
CREATE(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
CREATE(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
CREATE(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
CREATE(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
CREATE(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
CREATE(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
CREATE(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_013)
CREATE(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
CREATE(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_026)
CREATE(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_040)
CREATE(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_064)
CREATE(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
CREATE(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_017)
CREATE(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_159)
CREATE(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
CREATE(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
CREATE(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_089)
CREATE(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_157)
CREATE(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
CREATE(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
CREATE(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
CREATE(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
CREATE(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_051)
CREATE(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
CREATE(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
CREATE(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
CREATE(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_023)
CREATE(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
CREATE(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
CREATE(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
CREATE(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
CREATE(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
CREATE(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
CREATE(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
CREATE(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_154)
CREATE(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
CREATE(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
CREATE(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
CREATE(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
CREATE(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
CREATE(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_160)
CREATE(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_088)
CREATE(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
CREATE(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
CREATE(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_110)
CREATE(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_060)
CREATE(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
CREATE(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_030)
CREATE(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_170)
CREATE(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
CREATE(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
CREATE(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_004)
CREATE(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
CREATE(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_009)
CREATE(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
CREATE(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_077)
CREATE(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
CREATE(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
CREATE(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
CREATE(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
CREATE(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_053)
CREATE(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
CREATE(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
CREATE(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
CREATE(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_134)
CREATE(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
CREATE(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
CREATE(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
CREATE(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_048)
CREATE(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
CREATE(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
CREATE(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
CREATE(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
CREATE(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
CREATE(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_070)
CREATE(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_008)
CREATE(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
CREATE(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
CREATE(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_180)
CREATE(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_001)
CREATE(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
CREATE(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
CREATE(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
CREATE(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
CREATE(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
CREATE(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_100)
CREATE(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create5.feature
CREATE (:A)-[:R]->(:B)-[:R]->(:C)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create5.feature
CREATE (:A)<-[:R]-(:B)<-[:R]-(:C)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create5.feature
CREATE (:A)-[:R]->(:B)<-[:R]-(:C)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create5.feature
CREATE ()-[:R1]->()<-[:R2]-()-[:R3]->()

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create5.feature
CREATE (:A)<-[:R1]-(:B)-[:R2]->(:C)

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
CREATE (n:N {num: 42})
RETURN n
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
CREATE (n:N {num: 42})
RETURN n
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [42, 42, 42, 42, 42] AS x
CREATE (n:N {num: x})
RETURN n.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [42, 42, 42, 42, 42] AS x
CREATE (n:N {num: x})
RETURN n.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [1, 2, 3, 4, 5] AS x
CREATE (n:N {num: x})
WITH n
WHERE n.num % 2 = 0
RETURN n.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [1, 2, 3, 4, 5] AS x
CREATE (n:N {num: x})
RETURN sum(n.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [1, 2, 3, 4, 5] AS x
CREATE (n:N {num: x})
WITH sum(n.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
CREATE ()-[r:R {num: 42}]->()
RETURN r
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
CREATE ()-[r:R {num: 42}]->()
RETURN r
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [42, 42, 42, 42, 42] AS x
CREATE ()-[r:R {num: x}]->()
RETURN r.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [42, 42, 42, 42, 42] AS x
CREATE ()-[r:R {num: x}]->()
RETURN r.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [1, 2, 3, 4, 5] AS x
CREATE ()-[r:R {num: x}]->()
WITH r
WHERE r.num % 2 = 0
RETURN r.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [1, 2, 3, 4, 5] AS x
CREATE ()-[r:R {num: x}]->()
RETURN sum(r.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/create/Create6.feature
UNWIND [1, 2, 3, 4, 5] AS x
CREATE ()-[r:R {num: x}]->()
WITH sum(r.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
MATCH (n)
DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
MATCH (n)
DETACH DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
MATCH (n:X)
DETACH DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
OPTIONAL MATCH (n)
DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
OPTIONAL MATCH (a:DoesNotExist)
DELETE a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
OPTIONAL MATCH (n)
DETACH DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
MATCH (n:X)
DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete1.feature
MATCH (n)
DELETE n:Person

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete2.feature
MATCH ()-[r]-()
DELETE r

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete2.feature
MATCH (n)
OPTIONAL MATCH (n)-[r]-()
DELETE n, r

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete2.feature
MATCH p = ()-[r:T]-()
WHERE r.id = 42
DELETE r

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete2.feature
OPTIONAL MATCH ()-[r:DoesNotExist]-()
DELETE r
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete2.feature
MATCH ()-[r:T]-()
DELETE r:T

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete3.feature
MATCH p = (:X)-->()-->()-->()
DETACH DELETE p

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete3.feature
OPTIONAL MATCH p = ()-->()
DETACH DELETE p

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete4.feature
MATCH (a)-[r]-(b)
DELETE r, a, b
RETURN count(*) AS c

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete4.feature
MATCH (a)-[*]-(b)
DETACH DELETE a, b
RETURN count(*) AS c

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete4.feature
MATCH ()
CREATE (n)
DELETE n

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (:User)-[:FRIEND]->(n)
WITH collect(n) AS friends
DETACH DELETE friends[$friendIndex]

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (:User)-[r:FRIEND]->()
WITH collect(r) AS friendships
DETACH DELETE friendships[$friendIndex]

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (u:User)
WITH {key: u} AS nodes
DELETE nodes.key

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (:User)-[r]->(:User)
WITH {key: r} AS rels
DELETE rels.key

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (u:User)
WITH {key: collect(u)} AS nodeMap
DETACH DELETE nodeMap.key[0]

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (:User)-[r]->(:User)
WITH {key: {key: collect(r)}} AS rels
DELETE rels.key.key[0]

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH p = (:User)-[r]->(:User)
WITH {key: collect(p)} AS pathColls
DELETE pathColls.key[0], pathColls.key[1]

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH (a)
DELETE x

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete5.feature
MATCH ()
DELETE 1 + 1

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
DELETE n
RETURN 42 AS num
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
DELETE n
RETURN 42 AS num
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
DELETE n
RETURN 42 AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
DELETE n
RETURN 42 AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
WITH n, n.num AS num
DELETE n
WITH num
WHERE num % 2 = 0
RETURN num

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
WITH n, n.num AS num
DELETE n
RETURN sum(num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH (n:N)
WITH n, n.num AS num
DELETE n
WITH sum(num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
DELETE r
RETURN 42 AS num
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
DELETE r
RETURN 42 AS num
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
DELETE r
RETURN 42 AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
DELETE r
RETURN 42 AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
WITH r, r.num AS num
DELETE r
WITH num
WHERE num % 2 = 0
RETURN num

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
WITH r, r.num AS num
DELETE r
RETURN sum(num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/delete/Delete6.feature
MATCH ()-[r:R]->()
WITH r, r.num AS num
DELETE r
WITH sum(num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (a)-[:ADMIN]-(b)
WHERE a:A
RETURN a.id, b.id

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (a)<--()<--(b)-->()-->(c)
WHERE a:A
RETURN c

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (n)
WHERE n.name = 'Bar'
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (n:Person)-->()
WHERE n.name = 'Bob'
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH ()-[rel:X]-(a)
WHERE a.name = 'Andres'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (a)-[r]->(b)
WHERE b.name = $param
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (n {name: 'A'})-[r]->(x)
WHERE type(r) = 'KNOWS'
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (node)-[r:KNOWS]->(a)
WHERE r.name = 'monkey'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (a)-[r]->(b)
WHERE r.name = $param
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (n)
WHERE n.p1 = 12 OR n.p2 = 13
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (n)-[r]->(x)
WHERE type(r) = 'KNOWS' OR type(r) = 'HATES'
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH p = (n)-->(x)
WHERE length(p) = 1
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH p = (n)-->(x)
WHERE length(p) = 10
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (n)
MATCH r = (n)-[*]->()
WHERE r.name = 'apa'
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere1.feature
MATCH (a)
WHERE count(a) > 10
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere2.feature
MATCH (a)--(b)--(c)--(d)--(a), (b)--(d)
WHERE a.id = 1
  AND c.id = 2
RETURN d

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere2.feature
MATCH (advertiser)-[:ADV_HAS_PRODUCT]->(out)-[:AP_HAS_VALUE]->(red)<-[:AA_HAS_VALUE]-(a)
WHERE advertiser.id = $1
  AND a.id = $2
  AND red.name = 'red'
  AND out.name = 'product1'
RETURN out.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere3.feature
MATCH (a), (b)
WHERE a = b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere3.feature
MATCH (a:A), (b:B)
WHERE a.id = b.id
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere3.feature
MATCH (n)-[rel]->(x)
WHERE n.animal = x.animal
RETURN n, x

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere4.feature
MATCH (a), (b)
WHERE a <> b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere4.feature
MATCH (a), (b)
WHERE a.id = 0
  AND (a)-[:T]->(b:TheLabel)
  OR (a)-[:T*]->(b:MissingLabel)
RETURN DISTINCT b

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere5.feature
MATCH (:Root {name: 'x'})-->(i:TextNode)
WHERE i.var > 'te'
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere5.feature
MATCH (:Root {name: 'x'})-->(i:TextNode)
WHERE i.var > 'te' AND i:TextNode
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere5.feature
MATCH (:Root {name: 'x'})-->(i:TextNode)
WHERE i.var > 'te' AND i.var IS NOT NULL
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere5.feature
MATCH (:Root {name: 'x'})-->(i)
WHERE i.var > 'te' OR i.var IS NOT NULL
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (a)-->(b)
WHERE b:B
OPTIONAL MATCH (a)-->(c)
WHERE c:C
RETURN a.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (n:Single)
OPTIONAL MATCH (n)-[r]-(m)
WHERE m:NonExistent
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (n:Single)
OPTIONAL MATCH (n)-[r]-(m)
WHERE m.num = 42
RETURN m

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (n)-->(x0)
OPTIONAL MATCH (x0)-->(x1)
WHERE x1.name = 'bar'
RETURN x0.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (a1)-[r]->()
WITH r, a1
  LIMIT 1
OPTIONAL MATCH (a2)<-[r]-(b2)
WHERE a1 = a2
RETURN a1, r, b2, a2

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (x:X)
OPTIONAL MATCH (x)-[:E1]->(y:Y)
WHERE x.val < y.val
RETURN x, y

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (x:X)
OPTIONAL MATCH (x)-[:E1]->(y:Y)-[:E2]->(z:Z)
WHERE x.val < z.val
RETURN x, y, z

// ../../cypher-tck/tck-M23/tck/features/clauses/match-where/MatchWhere6.feature
MATCH (x:X)
OPTIONAL MATCH (x)-[:E1]->(y:Y)
OPTIONAL MATCH (y)-[:E2]->(z:Z)
WHERE x.val < z.val
RETURN x, y, z

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (a:A:B)
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (n {name: 'bar'})
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (n), (m)
RETURN n.num AS n, m.num AS m

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (n $param)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]->()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()<-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), ()-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(), ()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-(), ()-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[]-(), ()-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[]-(), ()-[r]-(), ()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[]-(), (), ()-[r]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), (a)-[q]-(b), (s), (s)-[r]->(t)<-[]-(b)
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[]->()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()<-[]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[*]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[*]->()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[]->()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()<-[]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[*]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[*]->()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-(), r = ()-[]-(), ()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[]-(), ()-[]-(), ()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()<-[]-(), r = ()-[]-()
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), r = (a)-[q]-(b), (s)-[p]-(t)-[]-(b)
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), (a)-[q]-(b), r = (s)-[p]-(t)-[]-(b)
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), (a)-[q]-(b), r = (s)-[p]->(t)<-[]-(b)
MATCH (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]->(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()<-[r]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-()-[]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r*]-()-[]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]->(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()<-[r]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(), (r)-[]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(), ()-[]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (s)-[r]-(t), (r)-[]-(t)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (s)-[r]-(t), (s)-[]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), ()-[r]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(), (), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[r]-(), (r), ()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-(), ()-[r]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[r]-(), ()-[]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[]-(), ()-[r]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[r]-(), (r), ()-[]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[r]-(), (), (r)-[]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()-[r*]-(), (r), ()-[]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[*]-()-[r]-(), (), (r)-[]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[*]-()-[r]-(), (), (r)-[*]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[*]-()-[r]-(), (), ()-[*]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), (a)-[r]-(b), (s), (s)-[]->(r)<-[]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[]->(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()<-[]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[*]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[*]->(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[]->(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()<-[]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[*]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (), r = ()-[*]->(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-(), r = ()-[]-(), (), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH r = ()-[]-(), ()-[]-(), (), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH ()-[]-()<-[]-(), r = ()-[]-(), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), r = (a)-[q]-(b), (s)-[p]-(t)-[]-(b), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), (a)-[q]-(b), r = (s)-[p]-(t)-[]-(b), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), (a)-[q]-(b), r = (s)-[p]->(t)<-[]-(b), (r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), r = (s)-[p]-(t)-[]-(b), (r), (a)-[q]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), r = (s)-[p]->(t)<-[]-(b), (r), (a)-[q]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), r = (s)-[p]-(t)-[]-(b), (a)-[q]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
MATCH (x), r = (s)-[p]->(t)<-[]-(b), (r)-[q]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH true AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH 123 AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH 123.4 AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH 'foo' AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH [] AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH [10] AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH {x: 1} AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match1.feature
WITH {x: []} AS n
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[r]->()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (:A)-[r]->(:B)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[r]-()
RETURN type(r) AS r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[r]->()
RETURN type(r) AS r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (node)-[r:KNOWS {name: 'monkey'}]->(a)
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (n)-[r:KNOWS|HATES]->(x)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (a1)-[r:T]->()
WITH r, a1
MATCH (a1)-[r:Y]->(b2)
RETURN a1, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[r:FOO $param]->()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]->()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)<-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]->(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()<-[]-(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]->(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)<-[]-(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r)-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-()-[*]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r)-[*]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r), ()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-(), ()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r), ()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(), (r)-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(), ()-[]-(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-(t), (s)-[]-(t)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s)-[]-(r), (s)-[]-(t)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s)-[]-(t), (r)-[]-(t)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s)-[]-(t), (s)-[]-(r)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s), (a)-[q]-(b), (r), (s)-[]-(t)-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s), (a)-[q]-(b), (r), (s)-[]->(t)<-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s), (a)-[q]-(b), (t), (s)-[]->(r)<-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[]->()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()<-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[*]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[*]->()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()<-[*]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[p*]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[p*]->()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()<-[p*]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (), r = ()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(), r = ()-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]->(), r = ()<-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()<-[]-(), r = ()-[]->()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[*]->(), r = ()<-[]-()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()<-[p*]-(), r = ()-[*]->()
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (x), (a)-[q]-(b), (r), (s)-[]->(t)<-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (x), (a)-[q]-(b), r = (s)-[p]->(t)<-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (x), (a)-[q*]-(b), r = (s)-[p]->(t)<-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (x), (a)-[q]-(b), r = (s)-[p*]->(t)<-[]-(b)
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[r]->()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)<-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[r]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[r]->(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)<-[r]-(r)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r)-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-()-[r*]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r)-[r*]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r), ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-(), ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH ()-[]-(r), ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r)-[]-(t), (s)-[r]-(t)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (s)-[]-(r), (s)-[r]-(t)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r), (a)-[q]-(b), (s), (s)-[r]-(t)-[]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (r), (a)-[q]-(b), (s), (s)-[r]->(t)<-[]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[]-(), ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = ()-[]-(), ()-[r*]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = (a)-[p]-(s)-[]-(b), (s)-[]-(t), (t), (t)-[r]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = (a)-[p]-(s)-[]-(b), (s)-[]-(t), (t), (t)-[r*]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH r = (a)-[p]-(s)-[*]-(b), (s)-[]-(t), (t), (t)-[r*]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (a)-[p]-(s)-[]-(b), r = (s)-[]-(t), (t), (t)-[r*]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (a)-[p]-(s)-[]-(b), r = (s)-[*]-(t), (t), (t)-[r]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
MATCH (a)-[p]-(s)-[]-(b), r = (s)-[*]-(t), (t), (t)-[r*]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH true AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH 123 AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH 123.4 AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH 'foo' AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH [] AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH [10] AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH {x: 1} AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match2.feature
WITH {x: []} AS r
MATCH ()-[r]-()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (n1)-[rel:KNOWS]->(n2)
RETURN n1, n2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[r]->(b)
RETURN a, r, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[r]-(b)
RETURN a, r, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH ()-[rel:KNOWS]->(x)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[r {name: 'r'}]-(b)
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-->(b:Foo)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (n:A:B:C:D:E:F:G:H:I:J:K:L:M)-[:T]->(m:Z:Y:X:W:V:U)
RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[:T|:T]->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (n)-->(a)-->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-->(b), (b)-->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[r]-(b)
RETURN a, r, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (n)-[r]-(n)
RETURN n, r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[r]->(b)
RETURN a, r, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (n)-[r]->(n)
RETURN n, r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (x:A)-[r1]->(y)-[r2]-(z)
RETURN x, r1, y, r2, z

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (x)-[r1]-(y)-[r2]-(z)
RETURN x, r1, y, r2, z

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[:A]->()-[:B]->(a)
RETURN a.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[:A]->(b), (b)-[:B]->(a)
RETURN a.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MATCH (a)-->(x)<-->(b)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'})
MATCH (a)-->(x), (b)-->(x), (c)-->(x)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a {name: 'a'}), (b {name: 'b'}), (c {name: 'c'})
MATCH (a)-->(x), (b)-->(x), (c)-->(x)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a {name: 'A'}), (c {name: 'C'})
MATCH (a)-->(b)
RETURN a, b, c

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-->(b)
MATCH (c)-->(d)
RETURN a, b, c, d

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a1)-[r:T]->()
WITH r, a1
MATCH (a1)-[r:T]->(b2)
RETURN a1, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a1)-[r]->()
WITH r, a1
MATCH (a1:X)-[r]->(b2)
RETURN a1, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a1:X:Y)-[r]->()
WITH r, a1
MATCH (a1:Y)-[r]->(b2)
RETURN a1, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
OPTIONAL MATCH (a)
WITH a
MATCH (a)-->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
OPTIONAL MATCH (a:TheLabel)
WITH a
MATCH (a)-->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (a)-[r]->()-[r]->(a)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match3.feature
MATCH (n)
WITH [n] AS users
MATCH (users)-->(messages)
RETURN messages

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a)-[r*1..1]->(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a {name: 'A'})-[*]->(x)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a {name: 'A'})-[:CONTAINS*0..1]->(b)-[:FRIEND*0..1]->(c)
RETURN a, b, c

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (n {var: 'start'})-[:T*]->(m {var: 'end'})
RETURN m

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a:A)
MATCH (a)-[r*2]->()
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH ()-[r:EDGE]-()
MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m)
RETURN count(p) AS c

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH ()-[r1]->()-[r2]->()
WITH [r1, r2] AS rs
  LIMIT 1
MATCH (first)-[rs*]->(second)
RETURN first, second

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a:A)
MATCH (a)-[:LIKES..]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match4.feature
MATCH (a:A)
MATCH (a)-[:LIKES*-2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*..]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*0]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*1]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*0..2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*1..2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*0..0]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*1..1]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*2..2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*2..1]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*1..0]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*..0]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*..1]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*..2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*0..]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*1..]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*2..]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*0]->()-[:LIKES]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES]->()-[:LIKES*0]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*1]->()-[:LIKES]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES]->()-[:LIKES*1]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES*2]->()-[:LIKES]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES]->()-[:LIKES*2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES]->()-[:LIKES*3]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)<-[:LIKES]-()-[:LIKES*3]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (a)-[:LIKES]->()<-[:LIKES*3]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (p)-[:LIKES*1]->()-[:LIKES]->()-[r:LIKES*2]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match5.feature
MATCH (a:A)
MATCH (p)-[:LIKES]->()-[:LIKES*2]->()-[r:LIKES]->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (a)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (a {name: 'A'})-->(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ({name: 'a'})<--({name: 'b'})
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (a:Label1)<--(:Label2)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (b)<--(a)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ({name: 'a'})-->({name: 'b'})
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (n)-->(k)<--(n)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (a:Label1)<--(:Label2)--()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (n)-->(m)--(o)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH path = (n)-->(m)--(o)--(p)
RETURN path

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p=(n)<-->(k)<-->(n)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (n)<-->(k)<--(n)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH topRoute = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End)
RETURN topRoute

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[*0..]->()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (n {name: 'A'})-[:KNOWS*1..2]->(x)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (a {name: 'A'})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (n:Movie)--(m)
RETURN p
  LIMIT 1

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ({name: 'A'})-[:KNOWS*..2]->()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ({name: 'A'})-[:KNOWS*..]->()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]->()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)<-[]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-(p)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]->(p)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()<-[]-(p)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]-(), ()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-(p), ()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]-()-[]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-(p)-[]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-()-[]-(p)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(p)-[]->(b), (t), (t)-[*]-(b)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r*]-(s)-[]-(b), (p), (t)-[]-(b)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(p)<-[*]-(b), (t), (t)-[]-(b)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]->()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()<-[p]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]->()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()<-[p*]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-(), ()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]-(), ()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-()-[]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]-()-[]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-()-[p]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-()-[p*]-()
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-()-[]->(b), (t), (t)-[p*]-(b)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r*]-(s)-[p]-(b), (t), (t)-[]-(b)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(s)<-[p]-(b), (t), (t)-[]-(b)
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (p)-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (p)-[]->()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = (p)<-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[]-(p)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[]->(p)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()<-[]-(p)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]->(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)<-[]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-(p), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]->(p), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()<-[]-(p), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]-(), (), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-(), (), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-(p), (), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (p)-[]-()-[]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-(p)-[]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-()-[]-(p), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(p)-[]-(b), p = (s)-[]-(t), (t), (t)-[]-(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(p)<-[*]-(b), p = (s)-[]-(t), (t), (t)-[]-(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[p]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[p]->()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()<-[p]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[p*]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()-[p*]->()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH p = ()<-[p*]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]->(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()<-[p]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]->(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()<-[p*]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-(), (), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]-(), (), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p]-()-[]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[p*]-()-[]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-()-[p]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH ()-[]-()-[p*]-(), p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(s)-[p]-(b), p = (s)-[]-(t), (t), (t)-[]-(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
MATCH (a)-[r]-(s)<-[p*]-(b), p = (s)-[]-(t), (t), (t)-[]-(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH true AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH 123 AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH 123.4 AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH 'foo' AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH [] AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH [10] AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH {x: 1} AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match6.feature
WITH {x: []} AS p
MATCH p = ()-[]-()
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
OPTIONAL MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (n)
OPTIONAL MATCH (n)-[:NOT_EXIST]->(x)
RETURN n, x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A), (b:C)
OPTIONAL MATCH (x)-->(b)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a1)-[r]->()
WITH r, a1
  LIMIT 1
OPTIONAL MATCH (a1)<-[r]-(b2)
RETURN a1, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH ()-[r]->()
WITH r
  LIMIT 1
OPTIONAL MATCH (a2)-[r]->(b2)
RETURN a2, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a1)-[r]->()
WITH r, a1
  LIMIT 1
OPTIONAL MATCH (a1)-[r]->(b2)
RETURN a1, r, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a {name: 'A'})
OPTIONAL MATCH (a)-[:KNOWS]->()-[:KNOWS]->(foo)
RETURN foo

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A), (c:C)
OPTIONAL MATCH (a)-->(b)-->(c)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:Single), (c:C)
OPTIONAL MATCH (a)-->(b)-->(c)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
OPTIONAL MATCH (a)
WITH a
OPTIONAL MATCH (a)-->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a)-[r {name: 'r1'}]-(b)
OPTIONAL MATCH (b)-[r2]-(c)
WHERE r <> r2
RETURN a, b, c

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:Single)
OPTIONAL MATCH (a)-[*]->(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:Single), (x:C)
OPTIONAL MATCH (a)-[*]->(x)
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:Single)
OPTIONAL MATCH (a)-[*3..]-(b)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A)
OPTIONAL MATCH (a)-[:FOO]->(b:B)
OPTIONAL MATCH (b)<-[:BAR*]-(c:B)
RETURN a, b, c

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A)
OPTIONAL MATCH p = (a)-[:X]->(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a {name: 'A'}), (x)
WHERE x.name IN ['B', 'C']
OPTIONAL MATCH p = (a)-->(x)
RETURN x, p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A), (b:B)
OPTIONAL MATCH p = (a)-[:X]->(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a {name: 'A'})
OPTIONAL MATCH p = (a)-->(b)-[*]->(c)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A), (b:B)
OPTIONAL MATCH p = (a)-[*]->(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
OPTIONAL MATCH (a:NotThere)
OPTIONAL MATCH (b:NotThere)
WITH a, b
OPTIONAL MATCH (b)-[r:NOR_THIS]->(a)
RETURN a, b, r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:Single)
OPTIONAL MATCH (a)-->(b:NonExistent)
OPTIONAL MATCH (a)-->(c:NonExistent)
WITH coalesce(b, c) AS x
MATCH (x)-->(d)
RETURN d

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:X)
OPTIONAL MATCH (a)-->(b:Y)
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:B)
OPTIONAL MATCH (a)-[r]-(a)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a)
WHERE NOT (a:B)
OPTIONAL MATCH (a)-[r]->(a)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (a:A), (b:B)
OPTIONAL MATCH (a)-->(x)
OPTIONAL MATCH (x)-[r]->(b)
RETURN x, r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
OPTIONAL MATCH (a:NotThere)
WITH a
MATCH (b:B)
WITH a, b
OPTIONAL MATCH (b)-[r:NOR_THIS]->(a)
RETURN a, b, r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (n:Single)
OPTIONAL MATCH (n)-[r]-(m:NonExistent)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (p:Player)-[:PLAYS_FOR]->(team:Team)
OPTIONAL MATCH (p)-[s:SUPPORTS]->(team)
RETURN count(*) AS matches, s IS NULL AS optMatch

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (p:Player)-[:PLAYS_FOR]->(team:Team)
OPTIONAL MATCH (p)-[s:SUPPORTS]->(team)
RETURN count(*) AS matches, s IS NULL AS optMatch

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match7.feature
MATCH (p:Player)-[:PLAYS_FOR]->(team:Team)
OPTIONAL MATCH (p)-[s:SUPPORTS]->(team)
RETURN count(*) AS matches, s IS NULL AS optMatch

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match8.feature
MATCH (a)
WITH a
MATCH (b)
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match8.feature
MATCH (a)
MERGE (b)
WITH *
OPTIONAL MATCH (a)--(b)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match8.feature
MATCH ()-->()
WITH 1 AS x
MATCH ()-[r1]->()<--()
RETURN sum(r1.times)

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH ()-[r*0..1]-()
RETURN last(r) AS l

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a)-[r:REL*2..2]->(b:End)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a)-[r:REL*2..2]-(b:End)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a:Start)-[r:REL*2..2]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a:Blue)-[r*]->(b:Green)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a)-[r1]->()-[r2]->(b)
WITH [r1, r2] AS rs, a AS first, b AS second
  LIMIT 1
MATCH (first)-[rs*]->(second)
RETURN first, second

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a)-[r1]->()-[r2]->(b)
WITH [r1, r2] AS rs, a AS second, b AS first
  LIMIT 1
MATCH (first)-[rs*]->(second)
RETURN first, second

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a:A), (b:B)
OPTIONAL MATCH (a)-[r*]-(b)
WHERE r IS NULL
  AND a <> b
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/match/Match9.feature
MATCH (a {name: 'A'}), (x)
WHERE x.name IN ['B', 'C']
OPTIONAL MATCH p = (a)-[r*]->(x)
RETURN r, x, p

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (a)
RETURN count(*) AS n

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (a:TheLabel)
RETURN labels(a)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (a:TheLabel)
RETURN a.id

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (a {num: 43})
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (a:TheLabel {num: 43})
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (a:TheLabel {num: 42})
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
CREATE (:X)
CREATE (:X)
MERGE (:X)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
WITH 42 AS var
MERGE (c:N {var: var})

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MATCH (foo)
WITH foo.x AS x, foo.y AS y
MERGE (:N {x: x, y: y + 1})
MERGE (:N {x: x, y: y})
MERGE (:N {x: x + 1, y: y})
RETURN x, y

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (test:L:B {num: 42})
RETURN labels(test) AS labels

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MATCH (person:Person)
MERGE (city:City {name: person.bornIn})

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
CREATE (a {num: 1})
MERGE ({v: a.num})

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE p = (a {num: 1})
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MATCH (a:A)
DELETE a
MERGE (a2:A)
RETURN a2.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MATCH (a)
MERGE (a)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE (n $param)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge1.feature
MERGE ({num: null})

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge2.feature
MERGE (a:TheLabel)
  ON CREATE SET a:Foo
RETURN labels(a)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge2.feature
MERGE (b)
  ON CREATE SET b.created = 1

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge2.feature
MERGE (a:TheLabel)
  ON CREATE SET a.num = 42
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge2.feature
MERGE (a:TheLabel)
  ON CREATE SET a.num = 42
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge2.feature
MATCH (person:Person)
MERGE (city:City)
  ON CREATE SET city.name = person.bornIn
RETURN person.bornIn

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge2.feature
MERGE (n)
  ON CREATE SET x.num = 1

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge3.feature
MERGE (a)
  ON MATCH SET a:L

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge3.feature
MERGE (a:TheLabel)
  ON MATCH SET a:Foo
RETURN labels(a)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge3.feature
MERGE (a:TheLabel)
  ON MATCH SET a.num = 42
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge3.feature
MATCH (person:Person)
MERGE (city:City)
  ON MATCH SET city.name = person.bornIn
RETURN person.bornIn

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge3.feature
MERGE (n)
  ON MATCH SET x.num = 1

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge4.feature
MATCH ()
MERGE (a:L)
  ON MATCH SET a:M1
  ON CREATE SET a:M2

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE]->(b)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE]->(b)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE]->(b)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a), (b)
MERGE (a)-[:X]->(b)
RETURN count(a)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE {name: 'r2'}]->(b)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE {name: 'r2'}]->(b)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)<-[r:TYPE]-(b)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE {name: 'Lola'}]->(b)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MERGE (a:A)
MERGE (b:B)
MERGE (a)-[:FOO]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MERGE (a {num: 1})
MERGE (b {num: 2})
MERGE p = (a)-[:R]->(b)
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a {id: 2}), (b {id: 1})
MERGE (a)-[r:KNOWS]-(b)
RETURN startNode(r).id AS s, endNode(r).id AS e

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a {id: 2}), (b {id: 1})
MERGE (a)-[r:KNOWS]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a {id: 2})--(b {id: 1})
MERGE (a)-[r:KNOWS]-(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a:Foo), (b:Bar)
WITH a, b
UNWIND ['a,b', 'a,b'] AS str
WITH a, b, split(str, ',') AS roles
MERGE (a)-[r:FB {foobar: roles}]->(b)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:T {numbers: [42, 43]}]->(b)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (n)
MATCH (m)
WITH n AS a, m AS b
MERGE (a)-[r:T]->(b)
RETURN a.id AS a, b.id AS b

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (n)
WITH n AS a, n AS b
MERGE (a)-[r:T]->(b)
RETURN a.id AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (n)
MATCH (m)
WITH n AS a, m AS b
MERGE (a)-[:T]->(b)
WITH a AS x, b AS y
MERGE (a)
MERGE (b)
MERGE (a)-[:T]->(b)
RETURN x.id AS x, y.id AS y

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (n)
WITH n AS a
MERGE (c)
MERGE (a)-[:T]->(c)
WITH a AS x
MERGE (c)
MERGE (x)-[:T]->(c)
RETURN x.id AS x

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C)
DELETE ab, bc, b, c
MERGE (newB:B {num: 1})
MERGE (a)-[:REL]->(newB)
MERGE (newC:C)
MERGE (newB)-[:REL]->(newC)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a)-[t:T]->(b)
DELETE t
MERGE (a)-[t2:T {name: 'rel3'}]->(b)
RETURN t2.name

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a:Foo)
MERGE (a)-[r:KNOWS]->(a:Bar)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a), (b)
MERGE (a)-->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a), (b)
MERGE (a)-[NO_COLON]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a), (b)
MERGE (a)-[:A|:B]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MATCH (a)-[r]->(b)
MERGE (a)-[r]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MERGE (a)
MERGE (b)
MERGE (a)-[r:FOO $param]->(b)
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
MERGE (a)
MERGE (b)
MERGE (a)-[:FOO*2]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge5.feature
CREATE (a), (b)
MERGE (a)-[r:X {num: null}]->(b)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge6.feature
MATCH (a:A), (b:B)
MERGE (a)-[:KNOWS]->(b)
  ON CREATE SET b.created = 1

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge6.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE]->(b)
  ON CREATE SET r.name = 'Lola'
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge6.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MERGE (a)-[r:TYPE]->(b)
  ON CREATE SET r.name = 'foo'

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge6.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MERGE (a)-[r:TYPE]->(b)
  ON CREATE SET r.name = null

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge6.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MERGE (a)-[r:TYPE]->(b)
  ON CREATE SET r = a

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge6.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MERGE (a)-[r:TYPE]->(b)
ON CREATE SET r += {name: 'bar', name2: 'baz'}

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge7.feature
MATCH (a:A), (b:B)
MERGE (a)-[:KNOWS]->(b)
  ON MATCH SET b.created = 1

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge7.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:KNOWS]->(b)
  ON MATCH SET r.created = 1

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge7.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE]->(b)
  ON MATCH SET r.name = 'Lola'
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge7.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MERGE (a)-[r:TYPE]->(b)
  ON MATCH SET r = a

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge7.feature
MATCH (a {name: 'A'}), (b {name: 'B'})
MERGE (a)-[r:TYPE]->(b)
  ON MATCH SET r += {name: 'baz', name2: 'baz'}

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge8.feature
MATCH (a:A), (b:B)
MERGE (a)-[r:TYPE]->(b)
  ON CREATE SET r.name = 'Lola'
  ON MATCH SET r.name = 'RUN'
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge9.feature
UNWIND [1, 2, 3, 4] AS int
MERGE (n {id: int})
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge9.feature
UNWIND ['Keanu Reeves', 'Hugo Weaving', 'Carrie-Anne Moss', 'Laurence Fishburne'] AS actor
MERGE (m:Movie {name: 'The Matrix'})
MERGE (p:Person {name: actor})
MERGE (p)-[:ACTED_IN]->(m)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge9.feature
CREATE (a:A), (b:B)
MERGE (a)-[:KNOWS]->(b)
CREATE (b)-[:KNOWS]->(c:C)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/merge/Merge9.feature
UNWIND [42] AS props
WITH props WHERE props > 32
WITH DISTINCT props AS p
MERGE (a:A {num: p})
RETURN a.num AS prop

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
MATCH (n)
REMOVE n.num
RETURN n.num IS NOT NULL AS still_there

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
MATCH (n)
REMOVE n.num, n.name
RETURN size(keys(n)) AS props

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
MATCH ()-[r]->()
REMOVE r.num
RETURN r.num IS NOT NULL AS still_there

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
MATCH ()-[r]->()
REMOVE r.num, r.a
RETURN size(keys(r)) AS props

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
OPTIONAL MATCH (a:DoesNotExist)
REMOVE a.num
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
MATCH (n)
OPTIONAL MATCH (n)-[r]->()
REMOVE r.num
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove1.feature
MATCH (n)
REMOVE n.num
RETURN sum(size(keys(n))) AS totalNumberOfProps

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove2.feature
MATCH (n)
REMOVE n:L
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove2.feature
MATCH (n)
REMOVE n:Foo
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove2.feature
MATCH (n)
REMOVE n:L1:L3
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove2.feature
MATCH (n)
REMOVE n:Bar
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove2.feature
OPTIONAL MATCH (a:DoesNotExist)
REMOVE a:L
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.num
RETURN n
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.num
RETURN n
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.name
RETURN n.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.name
RETURN n.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.name
WITH n
WHERE n.num % 2 = 0
RETURN n.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.name
RETURN sum(n.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n.name
WITH sum(n.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
RETURN n
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
RETURN n
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
RETURN n.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
RETURN n.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
WITH n
WHERE n.num % 2 = 0
RETURN n.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
RETURN sum(n.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH (n:N)
REMOVE n:N
WITH sum(n.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.num
RETURN r
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.num
RETURN r
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.name
RETURN r.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.name
RETURN r.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.name
WITH r
WHERE r.num % 2 = 0
RETURN r.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.name
RETURN sum(r.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/remove/Remove3.feature
MATCH ()-[r:R]->()
REMOVE r.name
WITH sum(r.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [true, false] AS bools
RETURN bools
ORDER BY bools

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [true, false] AS bools
RETURN bools
ORDER BY bools DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND ['.*', '', ' ', 'one'] AS strings
RETURN strings
ORDER BY strings

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND ['.*', '', ' ', 'one'] AS strings
RETURN strings
ORDER BY strings DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [1, 3, 2] AS ints
RETURN ints
ORDER BY ints

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [1, 3, 2] AS ints
RETURN ints
ORDER BY ints DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [1.5, 1.3, 999.99] AS floats
RETURN floats
ORDER BY floats

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [1.5, 1.3, 999.99] AS floats
RETURN floats
ORDER BY floats DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists
RETURN lists
ORDER BY lists

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists
RETURN lists
ORDER BY lists DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
MATCH p = (n:N)-[r:REL]->()
UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types
RETURN types
ORDER BY types

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy1.feature
MATCH p = (n:N)-[r:REL]->()
UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types
RETURN types
ORDER BY types DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN n.num AS prop
ORDER BY n.num

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN n.num AS prop
ORDER BY n.num DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN n.division, max(n.age)
  ORDER BY max(n.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (a)
RETURN DISTINCT a
  ORDER BY a.name

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (a)-->(b)
RETURN DISTINCT b
  ORDER BY b.name

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (a)
RETURN a, count(*)
ORDER BY count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN n.name, count(*) AS foo
  ORDER BY n.name

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN *
  ORDER BY n.id

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN DISTINCT n.id AS id
  ORDER BY id DESC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN DISTINCT n
  ORDER BY n.id

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (a:A), (b:X)
RETURN count(a) * 10 + count(b) * 5 AS x
ORDER BY x

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH p = (a)-[*]->(b)
RETURN collect(nodes(p)) AS paths, length(p) AS l
ORDER BY l

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (a)
RETURN DISTINCT a.name
  ORDER BY a.age

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy2.feature
MATCH (n)
RETURN n.num1
  ORDER BY max(n.num2)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy3.feature
MATCH (n)
RETURN n.division, count(*)
ORDER BY count(*) DESC, n.division ASC

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy4.feature
WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows
UNWIND prows AS p
UNWIND qrows[p] AS q
WITH p, count(q) AS rng
RETURN p
ORDER BY rng

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy4.feature
MATCH (c:Crew {name: 'Neo'})
WITH c, 0 AS relevance
RETURN c.rank AS rank
ORDER BY relevance, c.rank

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy5.feature
MATCH (n)
RETURN n.num AS n
ORDER BY n + 2

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy6.feature
MATCH (person)
RETURN avg(person.age) AS avgAge
ORDER BY $age + avg(person.age) - 1000

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy6.feature
MATCH (me: Person)--(you: Person)
RETURN me.age AS age, count(you.age) AS cnt
ORDER BY age, age + count(you.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy6.feature
MATCH (me: Person)--(you: Person)
RETURN me.age AS age, count(you.age) AS cnt
ORDER BY me.age + count(you.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy6.feature
MATCH (me: Person)--(you: Person)
RETURN count(you.age) AS agg
ORDER BY me.age + count(you.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-orderby/ReturnOrderBy6.feature
MATCH (me: Person)--(you: Person)
RETURN me.age + you.age, count(*) AS cnt
ORDER BY me.age + you.age + count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n)
RETURN n
ORDER BY n.name ASC
SKIP 2

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n)
RETURN n
ORDER BY n.name ASC
SKIP $skipAmount

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n)
WITH n SKIP toInteger(rand()*9)
WITH count(*) AS count
RETURN count > 0 AS nonEmpty

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n)
WHERE 1 = 0
RETURN n SKIP 0

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n) RETURN n SKIP n.count

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (p:Person)
RETURN p.name AS name
SKIP $_skip

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (p:Person)
RETURN p.name AS name
SKIP -1

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (p:Person)
RETURN p.name AS name
SKIP $_limit

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (p:Person)
RETURN p.name AS name
SKIP 1.5

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n)
RETURN n
  SKIP n.count

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit1.feature
MATCH (n)
RETURN n
  SKIP -1

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
UNWIND [1, 1, 1, 1, 1] AS i
RETURN i
LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (n)
RETURN n
ORDER BY n.name ASC
LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (n)
RETURN n
  LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
ORDER BY p.name
LIMIT 1

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
ORDER BY p.name
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (n)
WITH n LIMIT toInteger(ceil(1.7))
RETURN count(*) AS count

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (foo)
RETURN foo.num AS x
  ORDER BY x DESC
  LIMIT 4

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (a:A)-->(n)-->(m)
RETURN n.num, count(*)
  ORDER BY n.num
  LIMIT 1000

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (n) RETURN n LIMIT n.count

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
LIMIT $_limit

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
ORDER BY name LIMIT $_limit

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (n)
RETURN n
  LIMIT -1

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
LIMIT -1

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
LIMIT $_limit

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
ORDER BY name LIMIT $_limit

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (n)
RETURN n
  LIMIT 1.7

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit2.feature
MATCH (p:Person)
RETURN p.name AS name
LIMIT 1.5

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit3.feature
MATCH (n)
RETURN n
ORDER BY n.name ASC
SKIP 2
LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit3.feature
MATCH (n)
RETURN n
ORDER BY n.name ASC
SKIP $s
LIMIT $l

// ../../cypher-tck/tck-M23/tck/features/clauses/return-skip-limit/ReturnSkipLimit3.feature
MATCH (a)
RETURN a.count
  ORDER BY a.count
  SKIP 10
  LIMIT 10

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return1.feature
MATCH (n)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return1.feature
MATCH ()
RETURN foo

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
RETURN 1 + (2 - (3 * (4 / (5 ^ (6 % null))))) AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (a)
RETURN a.num

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (a)
RETURN a.name

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH ()-[r]->()
RETURN r.num

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH ()-[r]->()
RETURN r.name2

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (a)
RETURN a.num + 1 AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (a)
RETURN a.list2 + a.list1 AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (n)
RETURN (n:Foo)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
RETURN {a: 1, b: 'foo'}

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (a)
RETURN count(a) > 0

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (p:TheLabel)
RETURN p.id

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (n)-[r]->(m)
RETURN [n, r, m] AS r

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (n)-[r]->(m)
RETURN {node1: n, rel: r, node2: m} AS m

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH ()-[r]->()
DELETE r
RETURN type(r)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (n)
DELETE n
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (n)
DELETE n
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH ()-[r]->()
DELETE r
RETURN r.num

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return2.feature
MATCH (a)
RETURN foo(a)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return3.feature
MATCH (a)
RETURN a.id IS NOT NULL AS a, a IS NOT NULL AS b

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return3.feature
MATCH (a)
RETURN a.name, a.age, a.seasons

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return3.feature
MATCH (a)-[r]->()
RETURN a AS foo, r AS bar

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH (a)
WITH a.name AS a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH (a)
RETURN a AS ColumnName

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH (a)
RETURN a.id AS a, a.id

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH (n)
RETURN cOuNt( * )

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH p = (n)-->(b)
RETURN nOdEs( p )

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH p = (n)-->(b)
RETURN coUnt( dIstInct p )

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH p = (n)-->(b)
RETURN aVg(    n.aGe     )

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH ()
RETURN count(*) AS columnName

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH (a:A), (b:B)
RETURN coalesce(a.num, b.num) AS foo,
  b.num AS bar,
  {name: count(b)} AS baz

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
RETURN 1 AS a, 2 AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return4.feature
MATCH (person:Person)<--(message)<-[like]-(:Person)
WITH like.creationDate AS likeTime, person AS person
  ORDER BY likeTime, message.id
WITH head(collect({likeTime: likeTime})) AS latestLike, person AS person
RETURN latestLike.likeTime AS likeTime
  ORDER BY likeTime

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return5.feature
MATCH (n)
RETURN count(DISTINCT {name: n.list}) AS count

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return5.feature
MATCH (n)
RETURN DISTINCT n.name

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return5.feature
MATCH (n)
RETURN count(DISTINCT {name: [[n.list, n.list], [n.list, n.list]]}) AS count

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return5.feature
MATCH (n)
RETURN count(DISTINCT {name: [{name2: n.list}, {baz: {apa: n.list}}]}) AS count

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return5.feature
MATCH (a)
RETURN DISTINCT a.color, count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (n)
RETURN n.num AS n, count(n) AS count

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (a)
RETURN a, count(a) + 3

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (a)
WITH a.num AS a, count(*) AS count
RETURN count

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (n)
RETURN count(n) / 60 / 60 AS count

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (a)
RETURN size(collect(a))

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (a {name: 'Andres'})<-[:FATHER]-(child)
RETURN a.name, {foo: a.name='Andres', kids: collect(child.name)}

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (n)
RETURN n.num, count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH p=(a:L)-[*]->(b)
RETURN b, avg(length(p))

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH ()
RETURN count(*) * 10 AS c

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (n)
RETURN count(n), collect(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH ()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (a:L)-[rel]->(b)
RETURN a, count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH p = (a:T {name: 'a'})-[:R*]->(other:T)
WHERE other <> a
WITH a, other, min(length(p)) AS len
RETURN a.name AS name, collect(other.name) AS others, len

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
RETURN count(count(*))

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
RETURN count(rand())

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you)
WHERE me.name = 'Michael'
WITH me, count(DISTINCT r1) AS H1, count(DISTINCT r2) AS H2, you
MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you)
RETURN me, you, sum((1 - abs(r1.times / H1 - r2.times / H2)) * (r1.times + r2.times) / (H1 + H2)) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (person)
RETURN $age + avg(person.age) - 1000

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (me: Person)--(you: Person)
WITH me.age AS age, you
RETURN age, age + count(you.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (me: Person)--(you: Person)
RETURN me.age, me.age + count(you.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (me: Person)--(you: Person)
RETURN me.age + count(you.age)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return6.feature
MATCH (me: Person)--(you: Person)
RETURN me.age + you.age, me.age + you.age + count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return7.feature
MATCH p = (a:Start)-->(b)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return7.feature
MATCH ()
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/return/Return8.feature
MATCH (n)
WITH n
WHERE n.num = 42
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH (n:A)
WHERE n.name = 'Andres'
SET n.name = 'Michael'
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH (n:A)
WHERE n.name = 'Andres'
SET n.name = n.name + ' was here'
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH (n:A)
SET (n).name = 'neo4j'
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH ()-[r:REL]->()
SET (r).name = 'neo4j'
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH (n:A)
SET n.numbers = [1, 2, 3]
RETURN [i IN n.numbers | i / 2.0] AS x

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
CREATE (a {numbers: [1, 2, 3]})
SET a.numbers = a.numbers + [4, 5]
RETURN a.numbers

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
CREATE (a {numbers: [3, 4, 5]})
SET a.numbers = [1, 2] + a.numbers
RETURN a.numbers

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
OPTIONAL MATCH (a:DoesNotExist)
SET a.num = 42
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH (a)
SET a.name = missing
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
CREATE (a)
SET a.maplist = [{num: 1}]

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set1.feature
MATCH (n:X)
SET n.name = 'A', n.name2 = 'B', n.num = 5
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set2.feature
MATCH (n:A)
SET n.property1 = null
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set2.feature
MATCH (n)
WHERE n.name = 'Michael'
SET n.name = null
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set2.feature
MATCH ()-[r]->()
SET r.property1 = null
RETURN r

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n)
SET n:Foo
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n)
SET n:Foo:Bar
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n:A)
SET n:Foo
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n)
SET n:Foo:Bar
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n)
SET n :Foo
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n)
SET n :Foo :Bar
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
MATCH (n)
SET n :Foo:Bar
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set3.feature
OPTIONAL MATCH (a:DoesNotExist)
SET a:L
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set4.feature
MATCH (n:X)
SET n = {name: 'A', name2: 'B', num: 5}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set4.feature
MATCH (n:X {name: 'A'})
SET n = {name: 'B', baz: 'C'}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set4.feature
MATCH (n:X {name: 'A'})
SET n = {name: 'B', name2: null, baz: 'C'}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set4.feature
MATCH (n:X {name: 'A'})
SET n = { }
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set4.feature
OPTIONAL MATCH (a:DoesNotExist)
SET a = {num: 42}
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set5.feature
OPTIONAL MATCH (a:DoesNotExist)
SET a += {num: 42}
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set5.feature
MATCH (n:X {name: 'A'})
SET n += {name2: 'C'}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set5.feature
MATCH (n:X {name: 'A'})
SET n += {name2: 'B'}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set5.feature
MATCH (n:X {name: 'A'})
SET n += {name: null}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set5.feature
MATCH (n:X {name: 'A'})
SET n += { }
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = 43
RETURN n
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = 43
RETURN n
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = 42
RETURN n.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = 42
RETURN n.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = n.num + 1
WITH n
WHERE n.num % 2 = 0
RETURN n.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = n.num + 1
RETURN sum(n.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n.num = n.num + 1
WITH sum(n.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
RETURN n
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
RETURN n
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
RETURN n.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
RETURN n.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
WITH n
WHERE n.num % 2 = 0
RETURN n.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
RETURN sum(n.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH (n:N)
SET n:Foo
WITH sum(n.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = 43
RETURN r
LIMIT 0

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = 43
RETURN r
SKIP 1

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = 42
RETURN r.num AS num
SKIP 2 LIMIT 2

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = 42
RETURN r.num AS num
SKIP 0 LIMIT 5

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = r.num + 1
WITH r
WHERE r.num % 2 = 0
RETURN r.num AS num

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = r.num + 1
RETURN sum(r.num) AS sum

// ../../cypher-tck/tck-M23/tck/features/clauses/set/Set6.feature
MATCH ()-[r:R]->()
SET r.num = r.num + 1
WITH sum(r.num) AS sum
RETURN sum

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union1.feature
RETURN 1 AS x
UNION
RETURN 2 AS x

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union1.feature
RETURN 2 AS x
UNION
RETURN 1 AS x
UNION
RETURN 2 AS x

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union1.feature
UNWIND [2, 1, 2, 3] AS x
RETURN x
UNION
UNWIND [3, 4] AS x
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union1.feature
MATCH (a:A)
RETURN a AS a
UNION
MATCH (b:B)
RETURN b AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union1.feature
RETURN 1 AS a
UNION
RETURN 2 AS b

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union2.feature
RETURN 1 AS x
UNION ALL
RETURN 2 AS x

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union2.feature
RETURN 2 AS x
UNION ALL
RETURN 1 AS x
UNION ALL
RETURN 2 AS x

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union2.feature
UNWIND [2, 1, 2, 3] AS x
RETURN x
UNION ALL
UNWIND [3, 4] AS x
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union2.feature
MATCH (a:A)
RETURN a AS a
UNION ALL
MATCH (b:B)
RETURN b AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union2.feature
RETURN 1 AS a
UNION ALL
RETURN 2 AS b

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union3.feature
RETURN 1 AS a
UNION
RETURN 2 AS a
UNION ALL
RETURN 3 AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/union/Union3.feature
RETURN 1 AS a
UNION ALL
RETURN 2 AS a
UNION
RETURN 3 AS a

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND [1, 2, 3] AS x
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND range(1, 3) AS x
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
WITH [1, 2, 3] AS first, [4, 5, 6] AS second
UNWIND (first + second) AS x
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND RANGE(1, 2) AS row
WITH collect(row) AS rows
UNWIND rows AS x
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
MATCH (row)
WITH collect(row) AS rows
UNWIND rows AS node
RETURN node.id

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND $events AS event
MATCH (y:Year {year: event.year})
MERGE (e:Event {id: event.id})
MERGE (y)<-[:IN]-(e)
RETURN e.id AS x
ORDER BY x

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
WITH [[1, 2, 3], [4, 5, 6]] AS lol
UNWIND lol AS x
UNWIND x AS y
RETURN y

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND [] AS empty
RETURN empty

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND null AS nil
RETURN nil

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS duplicate
RETURN duplicate

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
WITH [1, 2, 3] AS list
UNWIND list AS x
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
MATCH (a:S)-[:X]->(b1)
WITH a, collect(b1) AS bees
UNWIND bees AS b2
MATCH (a)-[:Y]->(b2)
RETURN a, b2

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
WITH [1, 2] AS xs, [3, 4] AS ys, [5, 6] AS zs
UNWIND xs AS x
UNWIND ys AS y
UNWIND zs AS z
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/unwind/Unwind1.feature
UNWIND $props AS prop
MERGE (p:Person {login: prop.login})
SET p.name = prop.name
RETURN p.name, p.login

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [true, false] AS bools
WITH bools
  ORDER BY bools
  LIMIT 1
RETURN bools

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [true, false] AS bools
WITH bools
  ORDER BY bools DESC
  LIMIT 1
RETURN bools

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [1, 3, 2] AS ints
WITH ints
  ORDER BY ints
  LIMIT 2
RETURN ints

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [1, 3, 2] AS ints
WITH ints
  ORDER BY ints DESC
  LIMIT 2
RETURN ints

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [1.5, 1.3, 999.99] AS floats
WITH floats
  ORDER BY floats
  LIMIT 2
RETURN floats

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [1.5, 1.3, 999.99] AS floats
WITH floats
  ORDER BY floats DESC
  LIMIT 2
RETURN floats

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND ['.*', '', ' ', 'one'] AS strings
WITH strings
  ORDER BY strings
  LIMIT 2
RETURN strings

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND ['.*', '', ' ', 'one'] AS strings
WITH strings
  ORDER BY strings DESC
  LIMIT 2
RETURN strings

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists
WITH lists
  ORDER BY lists
  LIMIT 4
RETURN lists

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists
WITH lists
  ORDER BY lists DESC
  LIMIT 4
RETURN lists

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [date({year: 1910, month: 5, day: 6}),
        date({year: 1980, month: 12, day: 24}),
        date({year: 1984, month: 10, day: 12}),
        date({year: 1985, month: 5, day: 6}),
        date({year: 1980, month: 10, day: 24}),
        date({year: 1984, month: 10, day: 11})] AS dates
WITH dates
  ORDER BY dates
  LIMIT 2
RETURN dates

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [date({year: 1910, month: 5, day: 6}),
        date({year: 1980, month: 12, day: 24}),
        date({year: 1984, month: 10, day: 12}),
        date({year: 1985, month: 5, day: 6}),
        date({year: 1980, month: 10, day: 24}),
        date({year: 1984, month: 10, day: 11})] AS dates
WITH dates
  ORDER BY dates DESC
  LIMIT 2
RETURN dates

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [localtime({hour: 10, minute: 35}),
        localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
        localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}),
        localtime({hour: 12, minute: 35, second: 13}),
        localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123})] AS localtimes
WITH localtimes
  ORDER BY localtimes
  LIMIT 3
RETURN localtimes

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [localtime({hour: 10, minute: 35}),
        localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
        localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}),
        localtime({hour: 12, minute: 35, second: 13}),
        localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123})] AS localtimes
WITH localtimes
  ORDER BY localtimes DESC
  LIMIT 3
RETURN localtimes

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [time({hour: 10, minute: 35, timezone: '-08:00'}),
        time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}),
        time({hour: 12, minute: 31, second: 14, nanosecond: 645876124, timezone: '+01:00'}),
        time({hour: 12, minute: 35, second: 15, timezone: '+05:00'}),
        time({hour: 12, minute: 30, second: 14, nanosecond: 645876123, timezone: '+01:01'})] AS times
WITH times
  ORDER BY times
  LIMIT 3
RETURN times

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [time({hour: 10, minute: 35, timezone: '-08:00'}),
        time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}),
        time({hour: 12, minute: 31, second: 14, nanosecond: 645876124, timezone: '+01:00'}),
        time({hour: 12, minute: 35, second: 15, timezone: '+05:00'}),
        time({hour: 12, minute: 30, second: 14, nanosecond: 645876123, timezone: '+01:01'})] AS times
WITH times
  ORDER BY times DESC
  LIMIT 3
RETURN times

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12}),
        localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
        localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1}),
        localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999}),
        localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})] AS localdatetimes
WITH localdatetimes
  ORDER BY localdatetimes
  LIMIT 3
RETURN localdatetimes

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12}),
        localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}),
        localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1}),
        localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999}),
        localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})] AS localdatetimes
WITH localdatetimes
  ORDER BY localdatetimes DESC
  LIMIT 3
RETURN localdatetimes

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'}),
        datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'}),
        datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'}),
        datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'}),
        datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})] AS datetimes
WITH datetimes
  ORDER BY datetimes
  LIMIT 3
RETURN datetimes

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'}),
        datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'}),
        datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'}),
        datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'}),
        datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})] AS datetimes
WITH datetimes
  ORDER BY datetimes DESC
  LIMIT 3
RETURN datetimes

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH p = (n:N)-[r:REL]->()
UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types
WITH types
  ORDER BY types
  LIMIT 5
RETURN types

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH p = (n:N)-[r:REL]->()
UNWIND [n, r, p, 1.5, ['list'], 'text', null, false, 0.0 / 0.0, {a: 'map'}] AS types
WITH types
  ORDER BY types DESC
  LIMIT 5
RETURN types

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.bool AS bool
WITH a, bool
  ORDER BY bool
  LIMIT 3
RETURN a, bool

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.bool AS bool
WITH a, bool
  ORDER BY bool ASC
  LIMIT 3
RETURN a, bool

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.bool AS bool
WITH a, bool
  ORDER BY bool ASCENDING
  LIMIT 3
RETURN a, bool

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.bool AS bool
WITH a, bool
  ORDER BY bool DESC
  LIMIT 2
RETURN a, bool

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.bool AS bool
WITH a, bool
  ORDER BY bool DESCENDING
  LIMIT 2
RETURN a, bool

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num ASC
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num ASCENDING
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num DESC
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num DESCENDING
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num ASC
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num ASCENDING
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num DESC
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.num AS num
WITH a, num
  ORDER BY num DESCENDING
  LIMIT 3
RETURN a, num

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.name AS name
WITH a, name
  ORDER BY name
  LIMIT 3
RETURN a, name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.name AS name
WITH a, name
  ORDER BY name ASC
  LIMIT 3
RETURN a, name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.name AS name
WITH a, name
  ORDER BY name ASCENDING
  LIMIT 3
RETURN a, name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.name AS name
WITH a, name
  ORDER BY name DESC
  LIMIT 3
RETURN a, name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.name AS name
WITH a, name
  ORDER BY name DESCENDING
  LIMIT 3
RETURN a, name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.list AS list
WITH a, list
  ORDER BY list
  LIMIT 3
RETURN a, list

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.list AS list
WITH a, list
  ORDER BY list ASC
  LIMIT 3
RETURN a, list

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.list AS list
WITH a, list
  ORDER BY list ASCENDING
  LIMIT 3
RETURN a, list

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.list AS list
WITH a, list
  ORDER BY list DESC
  LIMIT 3
RETURN a, list

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.list AS list
WITH a, list
  ORDER BY list DESCENDING
  LIMIT 3
RETURN a, list

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.date AS date
WITH a, date
  ORDER BY date
  LIMIT 2
RETURN a, date

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.date AS date
WITH a, date
  ORDER BY date ASC
  LIMIT 2
RETURN a, date

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.date AS date
WITH a, date
  ORDER BY date ASCENDING
  LIMIT 2
RETURN a, date

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.date AS date
WITH a, date
  ORDER BY date DESC
  LIMIT 2
RETURN a, date

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.date AS date
WITH a, date
  ORDER BY date DESCENDING
  LIMIT 2
RETURN a, date

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time ASC
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time ASCENDING
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time DESC
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time DESCENDING
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time ASC
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time ASCENDING
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time DESC
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.time AS time
WITH a, time
  ORDER BY time DESCENDING
  LIMIT 3
RETURN a, time

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime ASC
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime ASCENDING
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime DESC
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime DESCENDING
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime ASC
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime ASCENDING
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime DESC
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a)
WITH a, a.datetime AS datetime
WITH a, datetime
  ORDER BY datetime DESCENDING
  LIMIT 3
RETURN a, datetime

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [0, 2, 1, 2, 0, 1] AS x
WITH x
  ORDER BY x ASC
  LIMIT 2
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [0, 2, 1, 2, 0, 1] AS x
WITH x
  ORDER BY x DESC
  LIMIT 2
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [0, 2, 1, 2, 0, 1] AS x
WITH DISTINCT x
  ORDER BY x ASC
  LIMIT 1
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
UNWIND [0, 2, 1, 2, 0, 1] AS x
WITH DISTINCT x
  ORDER BY x DESC
  LIMIT 1
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [true, false] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [351, -3974856, 93, -3, 123, 0, 3, -2, 20934587, 1, 20934585, 20934586, -10] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [351.5, -3974856.01, -3.203957, 123.0002, 123.0001, 123.00013, 123.00011, 0.0100000, 0.0999999, 0.00000001, 3.0, 209345.87, -10.654] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH ['Sort', 'order', ' ', 'should', 'be', '', 'consistent', 'with', 'comparisons', ', ', 'where', 'comparisons are', 'defined', '!'] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [[2, 2], [2, -2], [1, 2], [], [1], [300, 0], [1, -20], [2, -2, 100]] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [date({year: 1910, month: 5, day: 6}), date({year: 1980, month: 12, day: 24}), date({year: 1984, month: 10, day: 12}), date({year: 1985, month: 5, day: 6}), date({year: 1980, month: 10, day: 24}), date({year: 1984, month: 10, day: 11})] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [localtime({hour: 10, minute: 35}), localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876124}), localtime({hour: 12, minute: 35, second: 13}), localtime({hour: 12, minute: 30, second: 14, nanosecond: 645876123}), localtime({hour: 12, minute: 31, second: 15})] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [time({hour: 10, minute: 35, timezone: '-08:00'}), time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), time({hour: 12, minute: 31, second: 14, nanosecond: 645876124, timezone: '+01:00'}), time({hour: 12, minute: 35, second: 15, timezone: '+05:00'}), time({hour: 12, minute: 30, second: 14, nanosecond: 645876123, timezone: '+01:01'}), time({hour: 12, minute: 35, second: 15, timezone: '+01:00'})] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12}), localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1}), localdatetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999}), localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14})] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
WITH [datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'}), datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+00:17'}), datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, nanosecond: 1, timezone: '-11:59'}), datetime({year: 9999, month: 9, day: 9, hour: 9, minute: 59, second: 59, nanosecond: 999999999, timezone: '+11:59'}), datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '-11:59'})] AS values
WITH values, size(values) AS numOfValues
UNWIND values AS value
WITH size([ x IN values WHERE x < value ]) AS x, value, numOfValues
  ORDER BY value
WITH numOfValues, collect(x) AS orderedX
RETURN orderedX = range(0, numOfValues-1) AS equal

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY c
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY c ASC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY c ASCENDING
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY c DESC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY c DESCENDING
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY d ASC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY d ASCENDING
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY d DESC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy1.feature
MATCH (a:A), (b:B), (c:C)
WITH a, b
WITH a
  ORDER BY d DESCENDING
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY NOT (a.bool AND a.bool2)
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY NOT (a.bool AND a.bool2) ASC
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY NOT (a.bool AND a.bool2) ASCENDING
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY NOT (a.bool AND a.bool2) DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY NOT (a.bool AND a.bool2) DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num2 + (a.num * 2)) * -1
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num2 + (a.num * 2)) * -1 ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num2 + (a.num * 2)) * -1 ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num2 + (a.num * 2)) * -1 DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num2 + (a.num * 2)) * -1 DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num + a.num2 * 2) * -1.01
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num + a.num2 * 2) * -1.01 ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num + a.num2 * 2) * -1.01 ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num + a.num2 * 2) * -1.01 DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY (a.num + a.num2 * 2) * -1.01 DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.title + ' ' + a.name
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.title + ' ' + a.name ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.title + ' ' + a.name ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.title + ' ' + a.name DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.title + ' ' + a.name DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY [a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY [a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2 ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY [a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2 ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY [a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2 DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY [a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2 DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.date + duration({months: 1, days: 2})
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.date + duration({months: 1, days: 2}) ASC
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.date + duration({months: 1, days: 2}) ASCENDING
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.date + duration({months: 1, days: 2}) DESC
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.date + duration({months: 1, days: 2}) DESCENDING
  LIMIT 2
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6})
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6})
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.time + duration({minutes: 6}) DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6})
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6})
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) ASC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) ASCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) DESC
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a
  ORDER BY a.datetime + duration({days: 4, minutes: 6}) DESCENDING
  LIMIT 3
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a.name AS name
  ORDER BY a.name + 'C' ASC
  LIMIT 2
RETURN name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a.name AS name
  ORDER BY a.name + 'C' DESC
  LIMIT 2
RETURN name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a.name AS name, count(*) AS cnt
  ORDER BY a.name ASC
  LIMIT 1
RETURN name, cnt

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a.name AS name, count(*) AS cnt
  ORDER BY a.name DESC
  LIMIT 1
RETURN name, cnt

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a.name AS name, count(*) AS cnt
  ORDER BY a.name + 'C' ASC
  LIMIT 1
RETURN name, cnt

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH a.name AS name, count(*) AS cnt
  ORDER BY a.name + 'C' DESC
  LIMIT 1
RETURN name, cnt

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH DISTINCT a.name AS name
  ORDER BY a.name ASC
  LIMIT 1
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (a)
WITH DISTINCT a.name AS name
  ORDER BY a.name DESC
  LIMIT 1
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY count(1)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY count(n)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY count(n.num1)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY count(1 + n.num1)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) ASC
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) ASCENDING
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) DESC
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) DESCENDING
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2), n.name
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) ASC, n.name
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) ASCENDING, n.name
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) DESC, n.name
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY max(n.num2) DESCENDING, n.name
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name, max(n.num2)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name ASC, max(n.num2) ASC
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name ASC, max(n.num2) DESC
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name DESC, max(n.num2) ASC
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name DESC, max(n.num2) DESC
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name, max(n.num2), n.name2
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.name, n.name2, max(n.num2)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n, max(n.num2)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n.num1, max(n.num2)
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n, max(n.num2), n.num1
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy2.feature
MATCH (n)
WITH n.num1 AS foo
  ORDER BY n, count(n.num1), max(n.num2), n.num1
RETURN foo AS foo

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool, a.num
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool, a.num ASC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool, a.num ASCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASC, a.num
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASC, a.num ASC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASC, a.num ASCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASCENDING, a.num
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASCENDING, a.num ASC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASCENDING, a.num ASCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool, a.num DESC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool, a.num DESCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASC, a.num DESC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASC, a.num DESCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASCENDING, a.num DESC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool ASCENDING, a.num DESCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESC, a.num
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESC, a.num ASC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESC, a.num ASCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESCENDING, a.num
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESCENDING, a.num ASC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESCENDING, a.num ASCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESC, a.num DESC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESC, a.num DESCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESCENDING, a.num DESC
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.bool DESCENDING, a.num DESCENDING
  LIMIT 4
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num % 2 ASC, a.num, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num % 2 ASC, a.num, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num % 2 DESC, a.num, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num % 2 DESC, a.num, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) ASC, a.num ASC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) DESC, a.num ASC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, 4 + ((a.num * 2) % 2) ASC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, 4 + ((a.num * 2) % 2) DESC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, a.text ASC, 4 + ((a.num * 2) % 2) ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, a.text ASC, 4 + ((a.num * 2) % 2) DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) ASC, a.num ASC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) DESC, a.num ASC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, 4 + ((a.num * 2) % 2) ASC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, 4 + ((a.num * 2) % 2) DESC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, a.text DESC, 4 + ((a.num * 2) % 2) ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num ASC, a.text DESC, 4 + ((a.num * 2) % 2) DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) ASC, a.num DESC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) DESC, a.num DESC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, 4 + ((a.num * 2) % 2) ASC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, 4 + ((a.num * 2) % 2) DESC, a.text DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, a.text DESC, 4 + ((a.num * 2) % 2) ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, a.text DESC, 4 + ((a.num * 2) % 2) DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) ASC, a.num DESC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY 4 + ((a.num * 2) % 2) DESC, a.num DESC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, 4 + ((a.num * 2) % 2) ASC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, 4 + ((a.num * 2) % 2) DESC, a.text ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, a.text ASC, 4 + ((a.num * 2) % 2) ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
MATCH (a)
WITH a
  ORDER BY a.num DESC, a.text ASC, 4 + ((a.num * 2) % 2) DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a ASC, a DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a + 2 ASC, a + 2 DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a * a ASC, a * a DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a ASC, -1 * a ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY -1 * a DESC, a ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a DESC, a ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a + 2 DESC, a + 2 ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a * a DESC, a * a ASC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY a DESC, -1 * a DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
UNWIND [1, 2, 3] AS a
WITH a
  ORDER BY -1 * a ASC, a DESC
  LIMIT 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c ASC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c DESC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c, d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c ASC, d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c DESC, d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY c, a, d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY c ASC, a, d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY c DESC, a, d
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY c, d, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY b, c, d, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY c, b, c, d, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY c, d, b, b, d, c, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, e
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, e ASC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, e DESC
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, e, f
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, e ASC, f
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, e DESC, f
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY e, a, f
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY e ASC, a, f
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY e DESC, a, f
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY e, f, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY b, e, f, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY e, b, e, f, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY e, f, b, b, f, e, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c, e
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY a, c, e, b
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY b, c, a, f, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy3.feature
WITH 1 AS a, 'b' AS b, 3 AS c, true AS d
WITH a, b
WITH a
  ORDER BY d, f, b, b, f, c, a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum
  ORDER BY a.num + a.num2
  LIMIT 3
RETURN a, sum

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum
  ORDER BY sum
  LIMIT 3
RETURN a, sum

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum, a.num2 % 3 AS mod
  ORDER BY a.num2 % 3, a.num + a.num2
  LIMIT 3
RETURN a, sum, mod

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum, a.num2 % 3 AS mod
  ORDER BY a.num2 % 3, sum
  LIMIT 3
RETURN a, sum, mod

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum, a.num2 % 3 AS mod
  ORDER BY mod, a.num + a.num2
  LIMIT 3
RETURN a, sum, mod

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum, a.num2 % 3 AS mod
  ORDER BY mod, sum
  LIMIT 3
RETURN a, sum, mod

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num2 % 3 AS x
WITH a, a.num + a.num2 AS x
  ORDER BY x
  LIMIT 3
RETURN a, x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum
WITH a, a.num2 % 3 AS mod
  ORDER BY sum
  LIMIT 3
RETURN a, mod

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a.num2 AS x
WITH x % 3 AS x
  ORDER BY x
  LIMIT 3
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a.num2 AS x
WITH x % 3 AS x
  ORDER BY x * -1
  LIMIT 3
RETURN x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a.num2 % 3 AS mod, sum(a.num + a.num2) AS sum
  ORDER BY sum(a.num + a.num2)
  LIMIT 2
RETURN mod, sum

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a.num2 % 3 AS mod, sum(a.num + a.num2) AS sum
  ORDER BY sum
  LIMIT 2
RETURN mod, sum

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a, a.num + a.num2 AS sum
WITH a.num2 % 3 AS mod, min(sum) AS min
  ORDER BY sum(sum)
  LIMIT 2
RETURN mod, min

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a:A)
WITH a.num2 % 3 AS mod, min(a.num + a.num2) AS min
  ORDER BY sum(a.num + a.num2)
  LIMIT 2
RETURN mod, min

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (a)-[r]->(b:X)
WITH a, r, b, count(*) AS c
  ORDER BY c
MATCH (a)-[r]->(b)
RETURN r AS rel
  ORDER BY rel.id

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (person)
WITH avg(person.age) AS avgAge
ORDER BY $age + avg(person.age) - 1000
RETURN avgAge

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (me: Person)--(you: Person)
WITH me.age AS age, count(you.age) AS cnt
ORDER BY age, age + count(you.age)
RETURN age

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (me: Person)--(you: Person)
WITH me.age AS age, count(you.age) AS cnt
ORDER BY me.age + count(you.age)
RETURN age

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (me: Person)--(you: Person)
WITH count(you.age) AS agg
ORDER BY me.age + count(you.age)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-orderBy/WithOrderBy4.feature
MATCH (me: Person)--(you: Person)
WITH me.age + you.age, count(*) AS cnt
ORDER BY me.age + you.age + count(*)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit1.feature
MATCH (a)
WITH a.name AS property, a.num AS idToUse
  ORDER BY property
  SKIP 1
MATCH (b)
WHERE b.id = idToUse
RETURN DISTINCT b

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit1.feature
MATCH ()-[r1]->(x)
WITH x, sum(r1.num) AS c
  ORDER BY c SKIP 1
RETURN x, c

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit2.feature
MATCH (a:A)
WITH a
ORDER BY a.name
LIMIT 1
MATCH (a)-->(b)
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit2.feature
MATCH (a:Begin)
WITH a.num AS property
  LIMIT 1
MATCH (b)
WHERE b.id = property
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit2.feature
MATCH (n:A)
WITH n
LIMIT 1
MATCH (m:B), (n)-->(x:X)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit2.feature
MATCH ()-[r1]->(x)
WITH x, sum(r1.num) AS c
  ORDER BY c LIMIT 1
RETURN x, c

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit3.feature
MATCH (n)
WITH n
ORDER BY n.name ASC
SKIP 2
LIMIT 2
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit3.feature
MATCH (n)
WITH n
ORDER BY n.name ASC
SKIP $s
LIMIT $l
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/with-skip-limit/WithSkipLimit3.feature
MATCH (a)
WITH a.count AS count
  ORDER BY a.count
  SKIP 10
  LIMIT 10
RETURN count

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere1.feature
MATCH (a)
WITH a
WHERE a.name = 'B'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere1.feature
MATCH (a)
WITH DISTINCT a.name2 AS name
WHERE a.name2 = 'B'
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere1.feature
MATCH (a:A), (other:B)
OPTIONAL MATCH (a)-[r]->(other)
WITH other WHERE r IS NULL
RETURN other

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere1.feature
MATCH (other:B)
OPTIONAL MATCH (a)-[r]->(other)
WITH other WHERE a IS NULL
RETURN other

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere2.feature
MATCH (a)--(b)--(c)--(d)--(a), (b)--(d)
WITH a, c, d
WHERE a.id = 1
  AND c.id = 2
RETURN d

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere2.feature
MATCH (advertiser)-[:ADV_HAS_PRODUCT]->(out)-[:AP_HAS_VALUE]->(red)<-[:AA_HAS_VALUE]-(a)
WITH a, advertiser, red, out
WHERE advertiser.id = $1
  AND a.id = $2
  AND red.name = 'red'
  AND out.name = 'product1'
RETURN out.name

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere3.feature
MATCH (a), (b)
WITH a, b
WHERE a = b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere3.feature
MATCH (a:A), (b:B)
WITH a, b
WHERE a.id = b.id
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere3.feature
MATCH (n)-[rel]->(x)
WITH n, x
WHERE n.animal = x.animal
RETURN n, x

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere4.feature
MATCH (a), (b)
WITH a, b
WHERE a <> b
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere4.feature
MATCH (a), (b)
WITH a, b
WHERE a.id = 0
  AND (a)-[:T]->(b:TheLabel)
  OR (a)-[:T*]->(b:MissingLabel)
RETURN DISTINCT b

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere5.feature
MATCH (:Root {name: 'x'})-->(i:TextNode)
WITH i
WHERE i.var > 'te'
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere5.feature
MATCH (:Root {name: 'x'})-->(i:TextNode)
WITH i
WHERE i.var > 'te' AND i:TextNode
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere5.feature
MATCH (:Root {name: 'x'})-->(i:TextNode)
WITH i
WHERE i.var > 'te' AND i.var IS NOT NULL
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere5.feature
MATCH (:Root {name: 'x'})-->(i)
WITH i
WHERE i.var > 'te' OR i.var IS NOT NULL
RETURN i

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere6.feature
MATCH (a)-->()
WITH a, count(*) AS relCount
WHERE relCount > 1
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere7.feature
MATCH (a)
WITH a.name2 AS name
WHERE a.name2 = 'B'
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere7.feature
MATCH (a)
WITH a.name2 AS name
WHERE name = 'B'
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with-where/WithWhere7.feature
MATCH (a)
WITH a.name2 AS name
WHERE name = 'B' OR a.name2 = 'C'
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With1.feature
MATCH (a:A)
WITH a
MATCH (a)-->(b)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With1.feature
MATCH (a:A)
WITH a
MATCH (x:X), (a)-->(b)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With1.feature
MATCH ()-[r1]->(:X)
WITH r1 AS r2
MATCH ()-[r2]->()
RETURN r2 AS rel

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With1.feature
MATCH p = (a)
WITH p
RETURN p

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With1.feature
OPTIONAL MATCH (a:Start)
WITH a
MATCH (a)-->(b)
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With1.feature
OPTIONAL MATCH (a:A)
WITH a AS a
MATCH (b:B)
RETURN a, b

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With2.feature
MATCH (a:Begin)
WITH a.num AS property
MATCH (b)
WHERE b.id = property
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With2.feature
WITH {name: {name2: 'baz'}} AS nestedMap
RETURN nestedMap.name.name2

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With3.feature
MATCH (a)-[r]->(b:X)
WITH a, r, b
MATCH (a)-[r]->(b)
RETURN r AS rel
  ORDER BY rel.id

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
MATCH ()-[r1]->()
WITH r1 AS r2
RETURN r2 AS rel

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
MATCH (a:Begin)
WITH a.num AS property
MATCH (b:End)
WHERE property = b.num
RETURN b

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
MATCH (n)
WITH n.name AS n
RETURN n

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
WITH 1 AS a, 2 AS a
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
MATCH (a)
WITH a, count(*)
RETURN a

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
MATCH (person:Person)<--(message)<-[like]-(:Person)
WITH like.creationDate AS likeTime, person AS person
  ORDER BY likeTime, message.id
WITH head(collect({likeTime: likeTime})) AS latestLike, person AS person
WITH latestLike.likeTime AS likeTime
  ORDER BY likeTime
RETURN likeTime

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With4.feature
CREATE (m {id: 0})
WITH {first: m.id} AS m
WITH {second: m.first} AS m
RETURN m.second

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With5.feature
MATCH (a)
WITH DISTINCT a.name AS name
RETURN name

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With5.feature
MATCH (n)
WITH DISTINCT {name: n.list} AS map
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (a)
WITH a.name AS name, count(*) AS relCount
RETURN name, relCount

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH ()-[r1]->(:X)
WITH r1 AS r2, count(*) AS c
MATCH ()-[r2]->()
RETURN r2 AS rel

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (a)-[r1]->(b:X)
WITH a, r1 AS r2, b, count(*) AS c
MATCH (a)-[r2]->(b)
RETURN r2 AS rel

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH p = ()-[*]->()
WITH count(*) AS count, p AS p
RETURN nodes(p) AS nodes

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (person)
WITH $age + avg(person.age) - 1000 AS agg
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (me: Person)--(you: Person)
WITH me.age AS age, you
WITH age, age + count(you.age) AS agg
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (me: Person)--(you: Person)
WITH me.age AS age, me.age + count(you.age) AS agg
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (me: Person)--(you: Person)
WITH me.age + count(you.age) AS agg
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With6.feature
MATCH (me: Person)--(you: Person)
WITH me.age + you.age AS grp, me.age + you.age + count(*) AS agg
RETURN *

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With7.feature
MATCH (a:A)-[r:REL]->(b:B)
WITH a AS b, b AS tmp, r AS r
WITH b AS a, r
LIMIT 1
MATCH (a)-[r]->(b)
RETURN a, r, b

// ../../cypher-tck/tck-M23/tck/features/clauses/with/With7.feature
MATCH (david {name: 'David'})--(otherPerson)-->()
WITH otherPerson, count(*) AS foaf
WHERE foaf > 1
WITH otherPerson
WHERE otherPerson.name <> 'NotOther'
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation1.feature
MATCH (n)
RETURN n.name, count(n.num)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation1.feature
MATCH ()-[r]-()
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1, 2, 0, null, -1] AS x
RETURN max(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1, 2, 0, null, -1] AS x
RETURN min(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1.0, 2.0, 0.5, null] AS x
RETURN max(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1.0, 2.0, 0.5, null] AS x
RETURN min(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
RETURN max(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
RETURN min(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
RETURN max(i)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
RETURN min(i)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [[1], [2], [2, 1]] AS x
RETURN max(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [[1], [2], [2, 1]] AS x
RETURN min(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
RETURN max(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation2.feature
UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
RETURN min(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation3.feature
MATCH (n)
RETURN n.name, sum(n.num)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation3.feature
UNWIND range(1000000, 2000000) AS i
WITH i
LIMIT 3000
RETURN sum(i)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation5.feature
MATCH (n)
OPTIONAL MATCH (n)-[:NOT_EXIST]->(x)
RETURN n, collect(x)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation5.feature
OPTIONAL MATCH (f:DoesExist)
OPTIONAL MATCH (n:DoesNotExist)
RETURN collect(DISTINCT n.num) AS a, collect(DISTINCT f.num) AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileDisc(n.price, $percentile) AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileDisc(n.price, $percentile) AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileDisc(n.price, $percentile) AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileCont(n.price, $percentile) AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileCont(n.price, $percentile) AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileCont(n.price, $percentile) AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileCont(n.price, $param)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileCont(n.price, $param)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileCont(n.price, $param)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileDisc(n.price, $param)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileDisc(n.price, $param)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n)
RETURN percentileDisc(n.price, $param)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation6.feature
MATCH (n:S)
WITH n, size([(n)-->() | 1]) AS deg
WHERE deg > 2
WITH deg
LIMIT 100
RETURN percentileDisc(0.90, deg), deg

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation8.feature
OPTIONAL MATCH (a)
RETURN count(DISTINCT a)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation8.feature
MATCH (a)
RETURN count(DISTINCT a.name)

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation8.feature
UNWIND [null, null] AS x
RETURN collect(DISTINCT x) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/aggregation/Aggregation8.feature
UNWIND [null, 1, null] AS x
RETURN collect(DISTINCT x) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN true AND true AS tt,
       true AND false AS tf,
       true AND null AS tn,
       false AND true AS ft,
       false AND false AS ff,
       false AND null AS fn,
       null AND true AS nt,
       null AND false AS nf,
       null AND null AS nn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN true AND true AND true AS ttt,
       true AND true AND false AS ttf,
       true AND true AND null AS ttn,
       true AND false AND true AS tft,
       true AND false AND false AS tff,
       true AND false AND null AS tfn,
       true AND null AND true AS tnt,
       true AND null AND false AS tnf,
       true AND null AND null AS tnn,
       false AND true AND true AS ftt,
       false AND true AND false AS ftf,
       false AND true AND null AS ftn,
       false AND false AND true AS fft,
       false AND false AND false AS fff,
       false AND false AND null AS ffn,
       false AND null AND true AS fnt,
       false AND null AND false AS fnf,
       false AND null AND null AS fnn,
       null AND true AND true AS ntt,
       null AND true AND false AS ntf,
       null AND true AND null AS ntn,
       null AND false AND true AS nft,
       null AND false AND false AS nff,
       null AND false AND null AS nfn,
       null AND null AND true AS nnt,
       null AND null AND false AS nnf,
       null AND null AND null AS nnn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN true AND true AND true AND true AND true AND true AND true AND true AND true AND true AND true AS t,
       true AND true AND true AND false AND true AND true AND true AND true AND true AND true AND true AS tsf,
       true AND true AND true AND null AND true AND true AND true AND true AND true AND true AND true AS tsn,
       false AND false AND false AND false AND false AND false AND false AND false AND false AND false AND false AS f,
       false AND false AND false AND false AND true AND false AND false AND false AND false AND false AND false AS fst,
       false AND false AND false AND false AND false AND false AND null AND false AND false AND false AND false AS fsn,
       null AND null AND null AND null AND null AND null AND null AND null AND null AND null AND null AS n,
       null AND null AND null AND null AND true AND null AND null AND null AND null AND null AND null AS nst,
       null AND null AND null AND null AND false AND null AND null AND null AND null AND null AND null AS nsf,
       true AND false AND false AND false AND true AND false AND false AND true AND true AND true AND false AS m1,
       true AND true AND false AND false AND true AND false AND false AND true AND true AND true AND false AS m2,
       true AND true AND false AND false AND true AND null AND false AND true AND true AND null AND false AS m3

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
RETURN a, b, (a AND b) = (b AND a) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH a, b WHERE a IS NULL OR b IS NULL
RETURN a, b, (a AND b) IS NULL = (b AND a) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
UNWIND [true, false] AS c
RETURN a, b, c, (a AND (b AND c)) = ((a AND b) AND c) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
RETURN a, b, c, (a AND (b AND c)) IS NULL = ((a AND b) AND c) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 123 AND true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 123.4 AND false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 123.4 AND null

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 'foo' AND true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN [] AND false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN [true] AND false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN [null] AND null

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN {} AND true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN {x: []} AND true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN false AND 123

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN true AND 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN false AND 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN null AND 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN true AND []

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN true AND [false]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN null AND [null]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN false AND {}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN false AND {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 123 AND 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 123.4 AND 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN 'foo' AND {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN [true] AND [true]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean1.feature
RETURN {x: []} AND [123]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN true OR true AS tt,
       true OR false AS tf,
       true OR null AS tn,
       false OR true AS ft,
       false OR false AS ff,
       false OR null AS fn,
       null OR true AS nt,
       null OR false AS nf,
       null OR null AS nn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN true OR true OR true AS ttt,
       true OR true OR false AS ttf,
       true OR true OR null AS ttn,
       true OR false OR true AS tft,
       true OR false OR false AS tff,
       true OR false OR null AS tfn,
       true OR null OR true AS tnt,
       true OR null OR false AS tnf,
       true OR null OR null AS tnn,
       false OR true OR true AS ftt,
       false OR true OR false AS ftf,
       false OR true OR null AS ftn,
       false OR false OR true AS fft,
       false OR false OR false AS fff,
       false OR false OR null AS ffn,
       false OR null OR true AS fnt,
       false OR null OR false AS fnf,
       false OR null OR null AS fnn,
       null OR true OR true AS ntt,
       null OR true OR false AS ntf,
       null OR true OR null AS ntn,
       null OR false OR true AS nft,
       null OR false OR false AS nff,
       null OR false OR null AS nfn,
       null OR null OR true AS nnt,
       null OR null OR false AS nnf,
       null OR null OR null AS nnn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN true OR true OR true OR true OR true OR true OR true OR true OR true OR true OR true AS t,
       true OR true OR true OR false OR true OR true OR true OR true OR true OR true OR true AS tsf,
       true OR true OR true OR null OR true OR true OR true OR true OR true OR true OR true AS tsn,
       false OR false OR false OR false OR false OR false OR false OR false OR false OR false OR false AS f,
       false OR false OR false OR false OR true OR false OR false OR false OR false OR false OR false AS fst,
       false OR false OR false OR false OR false OR false OR null OR false OR false OR false OR false AS fsn,
       null OR null OR null OR null OR null OR null OR null OR null OR null OR null OR null AS n,
       null OR null OR null OR null OR true OR null OR null OR null OR null OR null OR null AS nst,
       null OR null OR null OR null OR false OR null OR null OR null OR null OR null OR null AS nsf,
       true OR false OR false OR false OR true OR false OR false OR true OR true OR true OR false AS m1,
       true OR true OR false OR false OR true OR false OR false OR true OR true OR true OR false AS m2,
       true OR true OR false OR false OR true OR null OR false OR true OR true OR null OR false AS m3

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
RETURN a, b, (a OR b) = (b OR a) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH a, b WHERE a IS NULL OR b IS NULL
RETURN a, b, (a OR b) IS NULL = (b OR a) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
UNWIND [true, false] AS c
RETURN a, b, c, (a OR (b OR c)) = ((a OR b) OR c) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
RETURN a, b, c, (a OR (b OR c)) IS NULL = ((a OR b) OR c) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 123 OR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 123.4 OR false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 123.4 OR null

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 'foo' OR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN [] OR false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN [true] OR false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN [null] OR null

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN {} OR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN {x: []} OR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN false OR 123

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN true OR 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN false OR 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN null OR 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN true OR []

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN true OR [false]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN null OR [null]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN false OR {}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN false OR {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 123 OR 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 123.4 OR 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN 'foo' OR {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN [true] OR [true]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean2.feature
RETURN {x: []} OR [123]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN true XOR true AS tt,
       true XOR false AS tf,
       true XOR null AS tn,
       false XOR true AS ft,
       false XOR false AS ff,
       false XOR null AS fn,
       null XOR true AS nt,
       null XOR false AS nf,
       null XOR null AS nn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN true XOR true XOR true AS ttt,
       true XOR true XOR false AS ttf,
       true XOR true XOR null AS ttn,
       true XOR false XOR true AS tft,
       true XOR false XOR false AS tff,
       true XOR false XOR null AS tfn,
       true XOR null XOR true AS tnt,
       true XOR null XOR false AS tnf,
       true XOR null XOR null AS tnn,
       false XOR true XOR true AS ftt,
       false XOR true XOR false AS ftf,
       false XOR true XOR null AS ftn,
       false XOR false XOR true AS fft,
       false XOR false XOR false AS fff,
       false XOR false XOR null AS ffn,
       false XOR null XOR true AS fnt,
       false XOR null XOR false AS fnf,
       false XOR null XOR null AS fnn,
       null XOR true XOR true AS ntt,
       null XOR true XOR false AS ntf,
       null XOR true XOR null AS ntn,
       null XOR false XOR true AS nft,
       null XOR false XOR false AS nff,
       null XOR false XOR null AS nfn,
       null XOR null XOR true AS nnt,
       null XOR null XOR false AS nnf,
       null XOR null XOR null AS nnn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN true XOR true XOR true XOR true XOR true XOR true XOR true XOR true XOR true XOR true XOR true AS t,
       true XOR true XOR true XOR false XOR true XOR true XOR true XOR true XOR true XOR true XOR true AS tsf,
       true XOR true XOR true XOR null XOR true XOR true XOR true XOR true XOR true XOR true XOR true AS tsn,
       false XOR false XOR false XOR false XOR false XOR false XOR false XOR false XOR false XOR false XOR false AS f,
       false XOR false XOR false XOR false XOR true XOR false XOR false XOR false XOR false XOR false XOR false AS fst,
       false XOR false XOR false XOR false XOR false XOR false XOR null XOR false XOR false XOR false XOR false AS fsn,
       null XOR null XOR null XOR null XOR null XOR null XOR null XOR null XOR null XOR null XOR null AS n,
       null XOR null XOR null XOR null XOR true XOR null XOR null XOR null XOR null XOR null XOR null AS nst,
       null XOR null XOR null XOR null XOR false XOR null XOR null XOR null XOR null XOR null XOR null AS nsf,
       true XOR false XOR false XOR false XOR true XOR false XOR false XOR true XOR true XOR true XOR false AS m1,
       true XOR true XOR false XOR false XOR true XOR false XOR false XOR true XOR true XOR true XOR false AS m2,
       true XOR true XOR false XOR false XOR true XOR null XOR false XOR true XOR true XOR null XOR false AS m3

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
RETURN a, b, (a XOR b) = (b XOR a) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH a, b WHERE a IS NULL OR b IS NULL
RETURN a, b, (a XOR b) IS NULL = (b XOR a) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
UNWIND [true, false] AS c
RETURN a, b, c, (a XOR (b XOR c)) = ((a XOR b) XOR c) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
RETURN a, b, c, (a XOR (b XOR c)) IS NULL = ((a XOR b) XOR c) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 123 XOR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 123.4 XOR false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 123.4 XOR null

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 'foo' XOR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN [] XOR false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN [true] XOR false

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN [null] XOR null

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN {} XOR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN {x: []} XOR true

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN false XOR 123

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN true XOR 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN false XOR 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN null XOR 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN true XOR []

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN true XOR [false]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN null XOR [null]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN false XOR {}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN false XOR {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 123 XOR 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 123.4 XOR 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN 'foo' XOR {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN [true] XOR [true]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean3.feature
RETURN {x: []} XOR [123]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT true AS nt, NOT false AS nf, NOT null AS nn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT NOT true AS nnt, NOT NOT false AS nnf, NOT NOT null AS nnn

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
MATCH (n)
WHERE NOT(n.name = 'apa' AND false)
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 0

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 1

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 123

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT ''

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 'false'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 'true'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT []

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [null]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [true]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [false]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [true, false]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [false, true]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [0]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [1]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [1, 2, 3]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [0.0]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [1.0]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT [1.0, 2.1]

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT ['']

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT ['', '']

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT ['true']

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT ['false']

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT ['a', 'b']

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: null}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: null}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: true}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: false}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {true: true}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {false: false}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {bool: true}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {bool: false}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: 0}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: 1}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 0}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 1}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 1, b: 2}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: 0.0}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: 1.0}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 0.0}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 1.0}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 1.0, b: 2.1}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {``: ''}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: ''}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 'a'}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 'a', b: 'b'}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean4.feature
RETURN NOT {a: 12, b: true}

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
UNWIND [true, false] AS c
RETURN a, b, c, (a OR (b AND c)) = ((a OR b) AND (a OR c)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
RETURN a, b, c, (a OR (b AND c)) IS NULL = ((a OR b) AND (a OR c)) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
UNWIND [true, false] AS c
RETURN a, b, c, (a AND (b OR c)) = ((a AND b) OR (a AND c)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
RETURN a, b, c, (a AND (b OR c)) IS NULL = ((a AND b) OR (a AND c)) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
UNWIND [true, false] AS c
RETURN a, b, c, (a AND (b XOR c)) = ((a AND b) XOR (a AND c)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
RETURN a, b, c, (a AND (b XOR c)) IS NULL = ((a AND b) XOR (a AND c)) IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
RETURN a, b, NOT (a OR b) = (NOT (a) AND NOT (b)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/boolean/Boolean5.feature
UNWIND [true, false] AS a
UNWIND [true, false] AS b
RETURN a, b, NOT (a AND b) = (NOT (a) OR NOT (b)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
WITH collect([0, 0.0]) AS numbers
UNWIND numbers AS arr
WITH arr[0] AS expected
MATCH (n) WHERE toInteger(n.id) = expected
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
WITH collect([0.5, 0]) AS numbers
UNWIND numbers AS arr
WITH arr[0] AS expected
MATCH (n) WHERE toInteger(n.id) = expected
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
WITH collect(['0', 0]) AS things
UNWIND things AS arr
WITH arr[0] AS expected
MATCH (n) WHERE toInteger(n.id) = expected
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH (a)
WITH a
MATCH (b)
WHERE a = b
RETURN count(b)

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH ()-[a]->()
WITH a
MATCH ()-[b]->()
WHERE a = b
RETURN count(b)

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN [1, 2] = [1] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN [null] = [1] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN ['a'] = [1] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN [[1]] = [[1], [null]] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN [[1], [2]] = [[1], [null]] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN [[1], [2, 3]] = [[1], [null]] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {} = {} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: true} = {k: true} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1} = {k: 1} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1.0} = {k: 1.0} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 'abc'} = {k: 'abc'} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 'a', l: 2} = {k: 'a', l: 2} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {} = {k: null} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: null} = {} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1} = {k: 1, l: null} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: null, l: 1} = {l: 1} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: null} = {k: null, l: null} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: null} = {k: null} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1} = {k: null} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1, l: null} = {k: null, l: null} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1, l: null} = {k: null, l: 1} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN {k: 1, l: null} = {k: 1, l: 1} AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN 0.0 / 0.0 = 1 AS isEqual, 0.0 / 0.0 <> 1 AS isNotEqual

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN 0.0 / 0.0 = 1.0 AS isEqual, 0.0 / 0.0 <> 1.0 AS isNotEqual

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN 0.0 / 0.0 = 0.0 / 0.0 AS isEqual, 0.0 / 0.0 <> 0.0 / 0.0 AS isNotEqual

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN 0.0 / 0.0 = 'a' AS isEqual, 0.0 / 0.0 <> 'a' AS isNotEqual

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN 1.0 = 1.0 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN 1 = 1.0 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN '1.0' = 1.0 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN '1' = 1 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH (p:TheLabel {id: 4611686018427387905})
RETURN p.id

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH (p:TheLabel)
WHERE p.id = 4611686018427387905
RETURN p.id

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH (p:TheLabel {id : 4611686018427387900})
RETURN p.id

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH (p:TheLabel)
WHERE p.id = 4611686018427387900
RETURN p.id

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH p1 = (:A)-->()
MATCH p2 = (:A)<--()
RETURN p1 = p2

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN null = null AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
RETURN null <> null AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison1.feature
MATCH (s)
WHERE s.name = undefinedVariable
  AND s.age = 10
RETURN s

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
MATCH (:Root)-->(i:Child)
WHERE i.var IS NOT NULL AND i.var > 'x'
RETURN i.var

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
MATCH (:Root)-->(i:Child)
WHERE i.var IS NULL OR i.var > 'x'
RETURN i.var

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
MATCH p = (n)-[r]->()
WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types
UNWIND range(0, size(types) - 1) AS i
UNWIND range(0, size(types) - 1) AS j
WITH types[i] AS lhs, types[j] AS rhs
WHERE i <> j
WITH lhs, rhs, lhs < rhs AS result
WHERE result
RETURN lhs, rhs

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
MATCH p = (n)-[r]->()
WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types
UNWIND range(0, size(types) - 1) AS i
UNWIND range(0, size(types) - 1) AS j
WITH types[i] AS lhs, types[j] AS rhs
WHERE i <> j
WITH lhs, rhs, lhs <= rhs AS result
WHERE result
RETURN lhs, rhs

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
MATCH p = (n)-[r]->()
WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types
UNWIND range(0, size(types) - 1) AS i
UNWIND range(0, size(types) - 1) AS j
WITH types[i] AS lhs, types[j] AS rhs
WHERE i <> j
WITH lhs, rhs, lhs >= rhs AS result
WHERE result
RETURN lhs, rhs

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
MATCH p = (n)-[r]->()
WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types
UNWIND range(0, size(types) - 1) AS i
UNWIND range(0, size(types) - 1) AS j
WITH types[i] AS lhs, types[j] AS rhs
WHERE i <> j
WITH lhs, rhs, lhs > rhs AS result
WHERE result
RETURN lhs, rhs

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN [1, 0] >= [1] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN [1, null] >= [1] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN [1, 2] >= [1, null] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN [1, 'a'] >= [1, null] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN [1, 2] >= [3, null] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN 0.0 / 0.0 > 1 AS gt, 0.0 / 0.0 >= 1 AS gtE, 0.0 / 0.0 < 1 AS lt, 0.0 / 0.0 <= 1 AS ltE

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN 0.0 / 0.0 > 1.0 AS gt, 0.0 / 0.0 >= 1.0 AS gtE, 0.0 / 0.0 < 1.0 AS lt, 0.0 / 0.0 <= 1.0 AS ltE

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN 0.0 / 0.0 > 0.0 / 0.0 AS gt, 0.0 / 0.0 >= 0.0 / 0.0 AS gtE, 0.0 / 0.0 < 0.0 / 0.0 AS lt, 0.0 / 0.0 <= 0.0 / 0.0 AS ltE

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN 0.0 / 0.0 > 'a' AS gt, 0.0 / 0.0 >= 'a' AS gtE, 0.0 / 0.0 < 'a' AS lt, 0.0 / 0.0 <= 'a' AS ltE

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN 1.0 < 1.0 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN 1 < 1.0 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN '1.0' < 1.0 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison2.feature
RETURN '1' < 1 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 1 < n.num < 3
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 1 < n.num <= 3
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 1 <= n.num < 3
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 1 <= n.num <= 3
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 'a' < n.name < 'c'
RETURN n.name

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 'a' < n.name <= 'c'
RETURN n.name

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 'a' <= n.name < 'c'
RETURN n.name

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 'a' <= n.name <= 'c'
RETURN n.name

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison3.feature
MATCH (n)
WHERE 10 < n.num <= 3
RETURN n.num

// ../../cypher-tck/tck-M23/tck/features/expressions/comparison/Comparison4.feature
MATCH (n)-->(m)
WHERE n.prop1 < m.prop1 = n.prop2 <> m.prop2
RETURN labels(m)

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional1.feature
MATCH (a)
RETURN coalesce(a.title, a.name)

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE -10
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 0
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 1
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 5
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 10
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 3000
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE -30
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 3
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 3001
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE '0'
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE true
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/conditional/Conditional2.feature
RETURN CASE 10.1
    WHEN -10 THEN 'minus ten'
    WHEN 0 THEN 'zero'
    WHEN 1 THEN 'one'
    WHEN 5 THEN 'five'
    WHEN 10 THEN 'ten'
    WHEN 3000 THEN 'three thousand'
    ELSE 'something else'
  END AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery1.feature
MATCH (n) WHERE exists {
  (n)-->()
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery1.feature
MATCH (n) WHERE exists {
  (n)-->(m) WHERE n.prop = m.prop
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery1.feature
MATCH (n) WHERE exists {
  (n)-[:NA]->()
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery1.feature
MATCH (n) WHERE exists {
  (n)-[r]->() WHERE type(r) = 'NA'
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery2.feature
MATCH (n) WHERE exists {
  MATCH (n)-->()
  RETURN true
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery2.feature
MATCH (n) WHERE exists {
  MATCH (n)-->(m)
  WITH n, count(*) AS numConnections
  WHERE numConnections = 3
  RETURN true
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery2.feature
MATCH (n) WHERE exists {
  MATCH (n)-->(m)
  SET m.prop='fail'
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery3.feature
MATCH (n) WHERE exists {
  MATCH (m) WHERE exists {
    (n)-[]->(m) WHERE n.prop = m.prop
  }
  RETURN true
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery3.feature
MATCH (n) WHERE exists {
  MATCH (m) WHERE exists {
    MATCH (l)<-[:R]-(n)-[:R]->(m) RETURN true
  }
  RETURN true
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/existentialSubqueries/ExistentialSubquery3.feature
MATCH (n) WHERE exists {
  MATCH (m) WHERE exists {
    MATCH (l) WHERE (l)<-[:R]-(n)-[:R]->(m) RETURN true
  }
  RETURN true
}
RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
CREATE (node)
RETURN labels(node)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
CREATE (node:Foo:Bar {name: 'Mattias'})
RETURN labels(node)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
CREATE (node :Foo:Bar)
RETURN labels(node)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
CREATE (n:Person)-[:OWNS]->(:Dog)
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
MATCH (n)
RETURN labels(n)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
MATCH (a)
WITH [a, 1] AS list
RETURN labels(list[0]) AS l

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
OPTIONAL MATCH (n:DoesNotExist)
RETURN labels(n), labels(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
MATCH p = (a)
RETURN labels(p) AS l

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph3.feature
MATCH (a)
WITH [a, 1] AS list
RETURN labels(list[1]) AS l

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH ()-[r]->()
RETURN type(r)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH ()-[r1]->()-[r2]->()
RETURN type(r1), type(r2)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH (a)
OPTIONAL MATCH (a)-[r:NOT_THERE]->()
RETURN type(r), type(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH (a)
OPTIONAL MATCH (a)-[r:T]->()
RETURN type(r)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH (a)-[r]->()
WITH [r, 1] AS list
RETURN type(list[0])

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [r, 0] | type(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [r, 1.0] | type(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [r, true] | type(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [r, ''] | type(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [r, []] | type(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph4.feature
MATCH (r)
RETURN type(r)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
RETURN a, a:B AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH ()-[r]->()
RETURN r, r:T2 AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
RETURN a, a:A:B AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
WHERE a:A:C
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
WHERE a:C:A
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
WHERE a:A:C:A
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
WHERE a:C:C:A
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (a)
WHERE a:C:A:A:C
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph5.feature
MATCH (n:Single)
OPTIONAL MATCH (n)-[r:TYPE]-(m)
RETURN m:TYPE

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
MATCH (n)
RETURN n.missing, n.missingToo, n.existing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
OPTIONAL MATCH (n)
RETURN n.missing, n.missingToo, n.existing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
OPTIONAL MATCH (n)
RETURN n.missing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
MATCH (n)
WITH [123, n] AS list
RETURN (list[1]).missing, (list[1]).missingToo, (list[1]).existing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
MATCH ()-[r]->()
RETURN r.missing, r.missingToo, r.existing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
OPTIONAL MATCH ()-[r]->()
RETURN r.missing, r.missingToo, r.existing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
OPTIONAL MATCH ()-[r]->()
RETURN r.missing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
MATCH ()-[r]->()
WITH [123, r] AS list
RETURN (list[1]).missing, (list[1]).missingToo, (list[1]).existing

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
WITH 123 AS nonGraphElement
RETURN nonGraphElement.num

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
WITH 42.45 AS nonGraphElement
RETURN nonGraphElement.num

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
WITH true AS nonGraphElement
RETURN nonGraphElement.num

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
WITH false AS nonGraphElement
RETURN nonGraphElement.num

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
WITH 'string' AS nonGraphElement
RETURN nonGraphElement.num

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph6.feature
WITH [123, true] AS nonGraphElement
RETURN nonGraphElement.num

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph7.feature
MATCH (n {name: 'Apa'})
RETURN n['nam' + 'e'] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph7.feature
CREATE (n {name: 'Apa'})
RETURN n['nam' + 'e'] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph7.feature
CREATE (n {name: 'Apa'})
RETURN n[$idx] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
MATCH (n)
UNWIND keys(n) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
MATCH (n)
UNWIND keys(n) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
MATCH (n)
UNWIND keys(n) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
OPTIONAL MATCH (n)
UNWIND keys(n) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
MATCH ()-[r:KNOWS]-()
UNWIND keys(r) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
MATCH ()-[r:KNOWS]-()
UNWIND keys(r) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
OPTIONAL MATCH ()-[r:KNOWS]-()
UNWIND keys(r) AS x
RETURN DISTINCT x AS theProps

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph8.feature
MATCH (n)
RETURN 'exists' IN keys(n) AS a,
       'missing' IN keys(n) AS b,
       'missingToo' IN keys(n) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
MATCH (p:Person)
RETURN properties(p) AS m

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
MATCH ()-[r:R]->()
RETURN properties(r) AS m

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
OPTIONAL MATCH (n:DoesNotExist)
OPTIONAL MATCH (n)-[r:NOT_THERE]->()
RETURN properties(n), properties(r), properties(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
RETURN properties({name: 'Popeye', level: 9001}) AS m

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
RETURN properties(1)

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
RETURN properties('Cypher')

// ../../cypher-tck/tck-M23/tck/features/expressions/graph/Graph9.feature
RETURN properties([true, false])

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
RETURN [1, 2, 3][0] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
RETURN [[1]][0][0]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[idx] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH ['Apa'] AS expr
RETURN expr[$idx] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[toInteger(idx)] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH true AS list, 0 AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH 123 AS list, 0 AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH 4.7 AS list, 0 AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH '1' AS list, 0 AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH [1, 2, 3, 4, 5] AS list, true AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH [1, 2, 3, 4, 5] AS list, 4.7 AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH [1, 2, 3, 4, 5] AS list, '1' AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH [1, 2, 3, 4, 5] AS list, [1] AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH [1, 2, 3, 4, 5] AS list, {x: 3} AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List1.feature
WITH $expr AS list, $idx AS idx
RETURN list[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1236, -1234) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1234, -1234) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-10, -3) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-10, 0) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1, 0) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -123) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 0) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 10) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(6, 10) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(1234, 1234) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(1234, 1236) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(1381, -3412, -1298) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -2000, -1298) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(10, -10, -3) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -10, -3) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -20, -2) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -10, -1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -1, -1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1236, -1234, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-10, 0, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1, 0, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, -123) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, -1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -123, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -1, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 0, -1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 0, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, 2) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 10, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(6, 10, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(1234, 1234, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(1234, 1236, 1) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-10, 0, 3) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-10, 10, 3) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-2000, 0, 1298) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-3412, 1381, 1298) AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
WITH 0 AS start, [1, 2, 500, 1000, 1500] AS stopList, [-1000, -3, -2, -1, 1, 2, 3, 1000] AS stepList
UNWIND stopList AS stop
UNWIND stepList AS step
WITH start, stop, step, range(start, stop, step) AS list
WITH start, stop, step, list, sign(stop-start) <> sign(step) AS empty
RETURN ALL(ok IN collect((size(list) = 0) = empty) WHERE ok) AS okay

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(2, 8, 0)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(2, 8, 0)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(2, 8, 0)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(2, 8, 0)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(true, 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, true, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, true)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-1.1, 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(-0.0, 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0.0, 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(1.1, 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -1.1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, -0.0, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 0.0, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1.1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, -1.1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, 1.1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range('xyz', 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 'xyz', 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, 'xyz')

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range([0], 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, [1], 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, [1])

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range({start: 0}, 1, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, {end: 1}, 1)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List11.feature
RETURN range(0, 1, {step: 1})

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH (a:Label1)
WITH collect(a) AS nodes
WITH nodes, [x IN nodes | x.name] AS oldNames
UNWIND nodes AS n
SET n.name = 'newName'
RETURN n.name, oldNames

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH (a:Label1)
WITH collect(a) AS nodes
WITH nodes, [x IN nodes WHERE x.name = 'original'] AS noopFiltered
UNWIND nodes AS n
SET n.name = 'newName'
RETURN n.name, size(noopFiltered)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH (n)
OPTIONAL MATCH (n)-[r]->(m)
RETURN size([x IN collect(r) WHERE x <> null]) AS cn

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH p = (n)-->()
RETURN [x IN collect(p) | head(nodes(x))] AS p

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH p = (n:A)-->()
WITH [x IN collect(p) | head(nodes(x))] AS p, count(n) AS c
RETURN p, c

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH (n)-->(b)
WHERE n.name IN [x IN labels(b) | toLower(x)]
RETURN b

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List12.feature
MATCH (n)
RETURN [x IN [1, 2, 3, 4, 5] | count(*)]

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3, 4, 5] AS list
RETURN list[1..3] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[1..] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[..2] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[0..1] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[0..0] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[-3..-1] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[3..1] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[-5..5] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[null..null] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[1..null] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[null..3] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[$from..$to] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List2.feature
WITH [1, 2, 3] AS list
RETURN list[$from..$to] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [1, 2] = 'foo' AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [1] = [1, null] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [1, 2] = [null, 'foo'] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [1, 2] = [null, 2] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [[1]] = [[1], [null]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [[1, 2], [1, 3]] = [[1, 2], [null, 'foo']] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List3.feature
RETURN [[1, 2], ['foo', 'bar']] = [[1, 2], [null, 'bar']] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List4.feature
RETURN [1, 10, 100] + [4, 5] AS foo

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List4.feature
RETURN [false, true] + false AS foo

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
WITH [[1, 2, 3]] AS list
RETURN 3 IN list[0] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 3 IN [[1, 2, 3]][0] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
WITH [1, 2, 3] AS list
RETURN 3 IN list[0..1] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 3 IN [1, 2, 3][0..1] AS r

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 1 IN ['1', 2] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [1, '2']] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1] IN [1, 2] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, 2] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1] IN [1, 2, [1]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [1, 2]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [2, 1]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [1, 2, 3]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [[1, 2]]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1, 2], [3, 4]] IN [5, [[1, 2], [3, 4]]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1, 2], 3] IN [1, [[1, 2], 3]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1]] IN [2, [[1]]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1, 3]] IN [2, [[1, 3]]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1]] IN [2, [1]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1, 3]] IN [2, [1, 3]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN null IN [null] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [null] IN [[null]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [null] IN [null] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1] IN [[1, null]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 3 IN [1, null, 3] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 4 IN [1, null, 3] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [[null, 'foo'], [1, 2]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [1, 2], null] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [[null, 'foo']] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [[null, 2]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [1, [1, 2, null]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2, null] IN [1, [1, 2, null]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [[null, 2], [1, 2]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[1, 2], [3, 4]] IN [5, [[1, 2], [3, 4], null]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [1, 2] IN [[null, 2], [1, 3]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [] IN [[]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [] IN [] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [] IN [1, []] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [] IN [1, 2] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[]] IN [1, [[]]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [] IN [1, 2, null] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN [[], []] IN [1, [[], []]] AS res

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 1 IN true

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 1 IN 123

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 1 IN 123.4

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 1 IN 'foo'

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List5.feature
RETURN 1 IN {x: []}

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
RETURN size([1, 2, 3]) AS n

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (n:TheLabel)
SET n.numbers = [1, 2, 3]
RETURN size(n.numbers)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
RETURN size([[], []] + [[]]) AS l

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
WITH null AS l
RETURN size(l), size(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH p = (a)-[*]->(b)
RETURN size(p)

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size(()--())

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size(()--(a))

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size((a)-->())

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size((a)<--(a {}))

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size((a)-[:REL]->(b))

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size((a)-[:REL]->(b))

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size((a)-[:REL]->(:C)<-[:REL]-(a {num: 5}))

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a), (b), (c)
RETURN size(()-[:REL*0..2]->()<-[:REL]-(:A {num: 5}))

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (n:X)
RETURN n, size([(n)--() | 1]) > 0 AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a:X)
RETURN size([(a)-->() | 1]) AS length

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a:X)
RETURN size([(a)-[:T]->() | 1]) AS length

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List6.feature
MATCH (a:X)
RETURN size([(a)-[:T|OTHER]->() | 1]) AS length

// ../../cypher-tck/tck-M23/tck/features/expressions/list/List9.feature
MATCH (n:TheLabel)
SET n.array = [1, 2, 3, 4, 5]
RETURN tail(tail(n.array))

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals1.feature
RETURN true AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals1.feature
RETURN TRUE AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals1.feature
RETURN false AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals1.feature
RETURN FALSE AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals1.feature
RETURN null AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals1.feature
RETURN NULL AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 372036854 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 9223372036854775807 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN -0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN -1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN -372036854 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN -9223372036854775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 9223372036854775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN -9223372036854775809 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 9223372h54775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals2.feature
RETURN 9223372#54775808 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x162CD4F6 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x7FFFFFFFFFFFFFFF AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN -0x0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN -0x1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN -0x162CD4F6 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN -0x8000000000000000 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x1a2b3c4d5e6f7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x1A2B3C4D5E6F7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x1A2b3c4D5E6f7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x1A2b3j4D5E6f7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x1A2b3c4Z5E6f7 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN 0x8000000000000000 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals3.feature
RETURN -0x8000000000000001 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN 0o1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN 0o2613152366 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN 0o777777777777777777777 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN 0o0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN -0o0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN -0o1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN -0o2613152366 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN -0o1000000000000000000000 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN 0o1000000000000000000000 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals4.feature
RETURN -0o1000000000000000000001 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 1.0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .1 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 3985764.3405892687 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .3405892687 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 126354186523812635418263552340512384016094862983471987543918591348961093487896783409268730945879405123840160948812635418265234051238401609486298347198754391859134896109348789678340926873094587962983471812635265234051238401609486298348126354182652340512384016094862983471987543918591348961093487896783409218.0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 0.0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -0.0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -.0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -126354186523812635418263552340512384016094862983471987543918591348961093487896783409268730945879405123840160948812635418265234051238401609486298347198754391859134896109348789678340926873094587962983471812635265234051238401609486298348126354182652340512384016094862983471987543918591348961093487896783409218.0 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 1e9 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 1E9 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .1e9 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 1e-5 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .1e-5 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN .1E-5 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -1e9 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -1E9 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -.1e9 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -1e-5 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -.1e-5 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN -.1E-5 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 1e308 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 123456789e300 AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals5.feature
RETURN 1.34E999

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN '' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN 'a' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN '🧐🍌❖⋙⚐' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN '\'' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN 'a\\bcn5t\'"\\//\\"\'' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN 'zvhg02LrjXbeIWUue4CzFT1baQ5ZA uP0ur4suuufFWZu3MGLlMUDYdhya1WcV8GcpEa4Pi03YjPieg2hJY3rt4OAQIeBKhpasUd' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN '92WeD0wBWj GWB1Y pUd6ZiCalZR5VJzIxXt6C74 4bfhdEAkXIHccJ4Avce2aWXTBj v22FvYQ4F0R GfPsbTyQYaL6DEHMbKR HlnP3BrpNBSO427Tsayra 950dNriiiRPbfLhV5oNHZl1Lbs44oAl40hU4LTkZkzIzNhwDtnOunSXwHH4FWpoqSP7B8VHz88z7X8BoSCECUIVs T4z5UFT9oPUCIsdTjzOocn8nT0dD7PVwRzsO2a4R5sNyYe6R4TdBqIWELcIiKhTpaMQsfuEPuzFnwCV1L g zZhhR7yNIo14oupUUD0V0oIHIRvtM0MITOkSiTTmO68ROtezWPfdJQq9pQ6gdcPsy YAU0wMs dVFBTyTzPml55k VOgY4dEuHUC5BkDGwCm8BTvls07JdY4cwm1zsLq1xGuQfVYmr62WF7VeVVIKFX3FuAIOyFqIshJxA8rTnEtzL1eSxrVcabZ0j24i1Zv2D6SDvsbs45pPHNollnZJmKUkLfrldZzlNEuy4JkJa2ahzizZW72f5m2xiwDKgM3 g7nrbYLgIKUtXOdoJeKgUl2cN7j4Xd30dajZpcIDBqsZ LwmRYQlvRXFafWBMD3yQfU4GEzbWQlxV6iBidK83UVdyyvMKaqPvdqovPVQzhIK Xfs yVwnSHDXpjUonwsOFeykee9TcixuxkbYp3Md EBk4LcBDn4zFR3JSmz3FGfP1llIGL ZYWHrzjugMbxPXU02OrqExStd X1ALxTJq2W6mO4kQig4ZQFKHIs66EVWf6HG3SKAxzPAmmf4DZmlZGawG agiO2PrNnWyifOau4em ozqdkAbxu6mCbMEjMri7dkzpjtYFwkxUGpgSjfDm481Eby3SKvwNybwvqfj5CXHWSjGpk8YtJV0T3jzNd731Wb3SWQrVyIy2Wz1UntzYJ33O W9cFnumIVZK1Sj0pQwWoxktNdyknjXiL5COyZiZDBJOcNtIXoklXdBDy' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN 'Qu7cFy732T2KJBCJzyY2xP7fWr4bhg7mdQALjUcVNa2nW2vIfAYMDxd4 ZGSe8g52kVWAiYI5K9SnVH2lMc7Uvh4M9hrvBUs5CPrAIjq9OwgxbVtZcfSrQgRe7hbkx162n0SNvY3KvqBBT5gyhTe4cG2BwJjFx8y11zpf0zyLpnYeQtd6V5maSx9tBigoLnjWdu9pjZ3aycAY8ZpzzOoBniPWThl1ydWyA8E4blXlzkeXnR9GY2UCpHpdmsg5u0GkF4phyqPt61 QRUiJBFXIHDx0zljppa vNLVbIaz8AqM7CGXU5796XKbiCX6uM9WRJXtUooJBJv0uHowr1tey4GQEL4t7j0tE4MznU9X7gRx7BMQGREyCBl5yR6qstIuMKug95TsVxUK3uE1oE5VsS68GlnL6IBAeNhsNMTA4kEflKNI2XKYGf4aDBLABvRa5Qbm12JpccslBbaILFQgQkPBy5nPRfh9Brjpyif1fPPkFB1rJIn 2z4G4irjFafOMuB 4JFTJnvj3 65yEbX7bNtgEF4oB7b7On8DVUAfFQfSz6T1SAFnOatwsNTts6dcH5JewU3jkS4TihfDUvAvw sjo0qoNxowKCoOtOUybt31Xg2mpeV5y5lyxZCSBkqjADNwLglwVcFa08Go3gU qP xs Hrw7ZmQ6vcy6oS6UH R3cJBUKWslkZKEYhXct3duSSWsnn8QFzKm6B4U6dmYXttjjVED0tqPXQ2vwp9eN8jJPebjZfT453810lZM9cQlfOhLdgsSaNaszT8t9pbPC5SrPPPIaXKF2IwRY3uMqAtTJD03bW o8dA3ZqT9igCrKRRfVo5j82HfUzjm2kBh4VT3UXfLGyTnnWqBqQ5WUbmdQQNfiMqGpBIcktEhov1XlJ6DyAzrn 1s yDyQS4Pjqg6y7NHl09nnJ3aMOxdDE7BHv4HVethC3Db32LHv6ZW9zotdOZ8tSH2AGKwhND6cfum67hSXu5OsAGeLZxrrMIn9ml9VWZj8Qxar 3lw3OM2jeUB62REWg7lxTJp3zVuaCQgejCGh40wOPR4vYtyzLdFxsxZ2qwn3XvnO2Xw25KckV8dstFfv4w9NFe03VTBWhoYkuSl0j3eCB1absxURBvss7ReatCgqonoVtkwD5RgknklJg12R56ikPOa9akQwEY ri5X8xDrKyqo2FXrj Np8AmXc4nx0yxydL4yF6WVk J9HmgHjGP0M3dMFOl0n15BUPyTAQNQhAHhDcGjt3jvTqKDW A4GG6gK2xn7hfdgAuoDj4h1lMZsSyYIGTnV6Zig8Nlmtwtss9kjCx 234UQbVuBD96JXbrjmY5jHd7c10KRvUFFzlGcdTscUUi38q6f0czcpoeT8MFBgEbrAw2b50fzz5tLhBGJGeKE0ndK64LOWP0olrS0voljEXYRiLMEArn1bkNUcaOgtQHzoV1Pqp6CR4suZxza66QcNOPH CoSuReOfjYOs1f0hWQ2RU2BUg1vJ5OyRPxAZ81195eJg82WgMFxIo 3EwNLUH6j3D41mu9G2L4ckbETdQRy8PEeM1KSIIjEBLD7xJdXFneolAbsv81mKzrWYRXw0pA8hTI4aIFFQSE8aaUkPUmCE0hzUENcHeNNHMK2UqsClOAdxRiz58hrzdUROac 7UM97kncRVWBSuW4GtISDrgBoEAJQqR2IFIh93W9wKCrESYtjf5uGLzEsGn3l0b2B0jXBoTkbd05jweOTk9LUOgpeNGBNWlpinKda9ny3OfjjCIZx3NnVqsxYiFeV0r4EgE4Vd5QypPNSoQN7rNx2aGufdT52tf1tGeK2d9uVgjDKIjJjZsDJhmnaOUbT5KPYb7fDJ4FJUcl22SMtXAkmQZTbXxGAkyve2SD6pyNB6ShBJ9LkeJPKDWQybSdRD tlQnHVqboE9iYdYOQSblltZwiQHMZcy4eiUHqW7uJ3Mve7bwRZLXYgJEoHeR7E8MXc0SpbVLpbEKEItiqFoi0XEhPGrRvE1PUhphlwiTJBXoLdGO02G97kpy2E8AZtFwboyuW0TXMyEg3bgAP TvGBrbtHyuYfbX6TC1meqTQOGTEMUBjz2VzRB ouL nUpSH7DojvQdxGi8F13xP12K 3IDVZX3UkPAsDgdChHvG5mFiSAaOWBZzUGbGTBkW52NtUQCMkzwYoCNwooNh5Ewk9rNafQQCsrmwaZQGrV pl4u9dBgedBtOeVF7SbxDdOewY uOb1TxLPn9CLwY7KY7igUGZ1prFMUqQ6IsmDLebpOIlG uKI7Xkar6hoRj1Xm8yWPf9o5qkGk agGuD4HrZOA2CtNVsWKiWnV09NLSBd5LdVkhjDbCFGRevIHO1aPCHTPpkml0EStzJdDHVtmGt6EYkbTXUZz7UZs8gKxNs950gEG 4Vtj98io9N0xNbO8FjLL lIqo4LunkmUs0otjT3gmshVAVTwQ0SjCRhqBs10NqVHAT9jCv J3s4mRSoirWeWw7UtzqRc bYtZrpvzmKvP 9lVvuOlEvWhcufv2VUQniDZFYE2EDtNCWrAqiodSAeX5eHEbfbQ5CwJjDjpBHJwoa7lPcZpt43nsXDLvZoIJZPzRPWOzDbt5u3loDI8aYrF2HOmpZ1Lrei XVV3DGYok8M5cWFgfaDILw8sa3kmDDJ2erUPblmMJZZB9eEOLnvEl5O9ALYbBBpnVTnLJvedw9uPVr1HXDmNWgAVpUFYXxKeQVReEFkHT29vENZGi3g7Bv2VUgEx5BxTlHGa13Kmge9QliYARWNfhBPjWQoP2ZRoKCDalsOCeohq2pNOKvkgZOy3AwfpFykBoUjtsvI7NAg6zVhCtCSo6PHcryDgAYYRF737e82qLpjkbCpMozebQRoGrZ7deTFTy TZCiOP2nGOKWiMnGq1daw3uAOx3ntthuZR1viQ8qmyXiIaBwJF5REqFJbZdPvRTpXns8vsG9PsXu DkPiWh3LieaiMGM3zyBsdFheatoBnj0ccBSsiKSDH SmVyBPw8K5vAeVA5WQy8LXX27mzhA7rlrXdWH8kMmtK15lR2AHE7XmSrzGaUbqWGRzmfrTDM vJPKZ8y73x8jhCvVK34nqFZbvlIRdYaUfWjQIGhdJ60V0JMJsh3bvYMDOlDnviPgT5MoAP6LszNwTp4O4yzdxgmq7CY48bQigcLRYEmg8ZWBU6ekc0Gk8Uuj3qC2Oy4DviJoC5Sy68xnl762KjXseDWuO0US6k5NCcztEWuB41AhFLjT Xlfv7dJNvDvyrTwYbnapgnqRTq2fD0NlkKq 0Wmjgv8HRMAUOU4Sfh2PNem 4BK4fBQKbzZWjK8Mjh4quPQr23P4K3qfVfyqGU9Y7HWPRiaz 86zjtl0Gu6DGo92GqPEGNBs RVMTebDPNWQWZju4bqF01z9jnsyzLbG1PD5bqdZccxHK9E bD9AM0KjsT3bSvhG4wCqIUOH9VBFKARnrscsgtF7sbmiBwtt3RfX9cddLMWn8lxh6swaE1pFyN8sg4qRhjVBHv0viacoxg7glAHAowSaqJXKRUWO0wBLz7esMhv9H44d6ztNLrgfays65REWjKWuMe4RsSP7VLGrQRvG6QKZ2GyI5K3WdQRRsPl2QrSxzCEHR1feQLSkngRpWAi4Gwt0ZUHzTGLMZeDQpG9fYWjSRfuPBWm4rHYyI0ny6WmqZa3yi3zeaHXKsNMMxV5RhI3wcY3UdgRBNTG1 yogATPH JYM5tSqE3M6tPgUumwH3qba 7a9XZcAJF7MYjb214yDndl8CYcQiJ9xUnyta9DToaXdLDFMOxIWdv4Oc Ae 092ASura8P5qig9RUZAwUpWiJTnCz6fSEkb1XHzAgW4HwrczuFFGsRNAUY5cReitkmwpFhf4Jz8KHHbUj8fbDROSfdsmjInlHnwLsB1sjfvZG6vk3LffL78GSIZ5fPfDnFm3rc2A0AWP0Abu539HMhSFd967byWCgpKqWCyMBjW1b6ool1XPus5gM0hx10WdSbMsEpYRR2SwicTxN18oIR4pJaQkE6or9TX6rz9vV6ZEyb4 ud wHyp1I227JdmFLT79kilRqj9K9xWnDR7SlCYSrIVavAnAa1vp4OF4fIQv5ER0Yj61PgmVQQWorwnGK4B9ArBshfyu CTzvR2isHgEpXVRg q2c4c4u7S19M 2PlDrcryc1M0HR1oBmdAsy mIV0E8BR 5E4xi5ZmrKMCXnpH7jURkiDLcu6bsOBufpLbEhKCaFJoC5r3nKY59nohuSWOigeOkEIcdCJt3VaQdwL1doyWzdpG0lUsCP9ZzzIB5oOp5RGgkoGiAh 5WSB5gHlpeK7lDPm2JEulXLeh97fRmSxe4nOVgyGscjoFfi9PgFqDuntZZwsNLiiMfsX8W 97fDeOT0TWvHw7JuioLjxDtOOOBrnZlKkUZQ7CRy7ch38tA1DzJOcCb178efuhtH91QrhoHJn6csVBRrg0DL98BGshITV Rojhsgq7j4NSLircpRgENiVRh49HigUtgwH5AK7xIAjMpD1ky gLFMqpfp4l9vlNrBhTpPDCI1R9UQMeCpiSXnJ9UjtL4uoXfmraI9xY4yVxVZFBXyhhk BaCRXp92qhUege4cIsMfK47FVJLIXzqn3Nu1TPmVyxQmmqXw7NLvVVu12x3DRrsi8ouiedz1KwDXmDhR4cLlnnHSei62MXC0elxELoUAooeyWnLPj6irfATHZ2BvdHUHNXLMq0xqqwzWDsQPklXiI5UPrCi6LfKDvwa38SAyF460vkacS92lPRdrh9S7xjhUOVN7mvjRYdnCU5I5sNiBsQqiuo8aA3GjQkXO0zBnddviQinlSjDEqB97aqZlviAgLTYtM8nbN1tWUH8gayIEPcpC4GyC37WCRiRg0hgyeXbs9sA1nHm5pIZ6sWY33A849nLfYF28C1TB27YPGTlrbCGIZEB4j62BvYUUAxmVo8VXS3hqegl2NPEKX8viEqv qwJZn1YBNjXRlJ1CHd6kqi48 udquQQT4XJTCMpfzbS9HOpXq4SRZmJDrqgXSsY4HPGc xk8p2ZRBodSSpKH3z6YOJ6tdOJ8BRqrymXoIsE1YK63BLSSyD437qwJedJzpHUMiLRZWJ 5FTcYrdWUIh4d I98rGjwjmlAdzEKMtXl0aimE 3hQ2T14pGWF2BlIKQPiX Q2FlSssswVhXtfdUdaBSlBXSk1e2JXVh4a2X5F ENUoTSbAgRHm 0jeYe9Mgw7BAOv1IXWzqfEpBgca0DnbIaDhYGojuvYb3ZKygKzsEXWF9ybgSNdMXARHYfNru2MoI9EKQHEcAHwwBWWKevcr92SnF83UyNyoyATmfb76bqggDHg0e4OD7FYyQ16VhLFowFGew7OhN16urh5 SU9JxECvjmbpe3mY83MOtZR65FRq3FaxYSsEDgI41Ce3wsNgkUXaxmiUw8M6FUFwihz8ZEihfxMb41EAnafjOUo66tfs1bzzWFvGuuEXfLeHOs07YF7YSmwhs6smrP3SkWXJCQfEjr9kn8sGB2VBpmO7aTiIdGHBa2u hyjkJrTu64n54dknHBPMl2Yc nyEoHucwalDRjPBhPNTAenytix29MsVEFvnaEqgxkB1DbdbifGvkWAt9t86BWvbgE2hIPAGA6zcm43Wzg8ENZCLqVoGSAFe ZjpptB4c84l a1XxUUxo7fmmDdkFNaTZP6UFmkzFnhDt3NB Dzom5Px h5CEHIvdgRSbdBr9tlLkm9gBTbS3fTYjPTPBnnGyUZnOhLMS8CExBvaAdxh6lmprWxyfaLOfi4uqmDQ5VGmjexWZin2Q7QQBSDZaLoSImoZ0TytdMvwpdIHQysLtvdLUJ9Jmklz4C cwZM538cCfD97iMjkZ sGB95sShsGhgNCUwR35cmjMJfVuFtppu4iU3AZkXs0OyKFUxBMhLEHQYBM0U9H rV0rHJDW0LirrncRqtLBOvcj bC4jKiSN3slzd v2XbmKBd4tWKKLcgMZmtF99WcteKyYMCWkF62nBVTyZZsyxUWETHOB9O2B7dukuQuGFz28pQhR Qsf7xKo8cwjc66YYWj61OFt4qFO9miVOojp8MR2qhCXdl1tVVHoUPh8WnrEnPWT C9u5co4NUhSAUHwyPuMKbr jhx9u34vJNaAScYvGDKy3wmxB3ogzfWE7n yqN1RvxJl9 mc0vk3ObjaGUYidas4nK2fQaVeNvwebbr dHeLJF0f qHWUoJmBKg6d7owotrQ7beZcYO7J7vZRZv0P26JuM3he8Q hl2Lak9ViLes59a4zfOn rzS9swYagFbPhwll44Q7lfRQzbjs7OO6viaC3aCYPv5BAPB8F9k W6sKpfuY52rpez5W4LoBBmjYMz8j 9Sc5WPXj32Zic fCaM65d eFACBAwnQeJKohksmmx9GPBKEZScTHe0gVqOfKklUv7OITLOVFIXD311e8KoWg2L7RZgiWz1JHNPI1BL9jkY3aQW52b6OGDX LR HQf7WoT3lQF85ICLNVKbjzWUDEL2AOIWK0jxvTnFiDBH7y2b4MpfmAfWBXtUsJJfgUGG2VW3pTFOqQS6rWir6jfvQs43ohSyt68RiZ1CfbR0Y9xY04fWPVsLKRlo9KM4JllXAwwKuSbvRpT4amOtbdkdKEKDPvmA6FQ61cSWayEADwjN8lbpUELdl150T9MjcDDdWZxv7nZ XAj493l8tUZlVGNXZ7OxOyoTf3PyIDCdtN9ut7TDBzpIFlDQhSBAHDY5cs5ct9nLzA6s1DGqdBj4NJPeRiKsPYGHnyqK5CE8S9IAJ 0XIfiJR so8fY9iySAKKECppnRk4hcdoVQhevjFBqAbSG02X1zkaKRXpvGxdWryFYL6TA9fVvRNpwi3JVSnhLslULMTcsnZeIkwN7QHWLDWh29DPXX31g7lLYdYnkiA53ZCCN0EKuwEpToy84vh3Gu8sO6Kv k6tHynKAVz0SentHsh 0LV387w8PQHYdYn7PzsQJ1sNmqIOyTn4Te7z1ElCSgqU0I0ImflD ilxsSUrsqaqhofXMyDkb5ZAaYGtFrhn Ea6 qw5ZCkbws8N8aY4gW90e90k9Rhhg0vE5nD74Rg5awiOA7vtmjn9LOKdLF67j1nVrpIZU4ADStXLwHWX0yCRFdw sfEKYuIrnFOc1sSjOKx fvHOSVGlYqaBv1yKqRBheU hsYupfxA3zzrlsYD71qZ4TmlqayGtK8p5SELT1mD0YG0v9VYPQrSqkrk V4kcPKckonY7zPZKkYbf6b5e22XVE0AWokBiYQwNuyIqEifpkhlc9PrUp13cwWncTlnMWyRDQrlW2i6oRJbMZJoE2Bcy72YMzbqvbcrmXnemI9tUDiHRZi0V1gbtxxvEjw 0 Z5UjDGk0jua35FOBRL4DdYRIawvkbzo7Lr 4PymJ0DrUu3k5IvBhQthdDJG7Dpf8Q4AiyUsZKkied3d7CFLKcpAmZ7up8J0pOcGEN3q0HsIUJ m1oW3acBCBXiYJ2 n JKAteFJPTgCqQzDhNOootC6BJXq4Ju4VUSdfD8poERjuadKYrInUCTKqRgU6H7N8B2lILyF GKnUT4mrxGxDduPrMIKE1wIdCOwAlD7H5V BYKZDF3GGwxsRU9Ktctq3tgatYQyB40VkWSftduesDqH118 2MhhZqYFwq8stqRqhFpYsjHwqY1owy yPnApsBOt7F7P9Y2NPCBziPywkY7nZiRhf2UtSLpWGPWlegIlkMCYtOB fNnPpxotXpOyUiNWcF TpwXxXrUG2PTnHouO2vtQOSS5OkbpDYPMgCNZI Pvc6WAV8H61FnNOaGJHYY8zmKGMNaqZg4XRpbDZKCd34aFJDmu6rXwzOf4LqagfuR6S3shK82phsJvJXpho6pkugIfCiai0Xw9qkUW2NT4DMiomcJmWEwUCnTEsZCUSN0Lxlz6Cm49 Jc8OBtlCYqGwOtQkK2Uqz0CYGxX9zUcu BYH2I00luXU6seC2vcn2ouX3oBmOkfg5GW4whSQJd0ahBvsRAvHMj2YAixGkZM9XE FgJqJYl98YoIUQtH7aOXkZfcgWsojqGo0v8DdZNjYuXJzUEgDzIbD xWwxjf2S1LeLieYDcqgnu6I6WpMlwaCAtReo tY7mLd5r2oxLABi7epYW6oZZrYxwhjZZNw1FgOo1OEWfwKn ApeXjiXDrQZb5rhwEjKGOE5uzI6Qohv3LIQgbBUL8rFU3g9FmkmmfdVtMGPpolkueiFzm4maKb8X4LLGiZ PeQfMGFQBW7UzH9PJFsVHecq96W6MVn6xbIiRItnuce61JXf7YWslpM1ktrFVzEF2hyEJSoMAec1Z3z2rEm33CBtOF9snfBky2ePmnioOm1yE8FpkyK7DVXGQEER2Zpz4nBGUalgPCNTQcOf34D4IY2Ucbn5 qMJzF5ibH0ogr6QmeSyRMQ3gWRp92RVpxD5sWQwKoCIagfhxevuLhz5k59zJqW5p82zcGiC3hcf3mMuJJ0IVibzNgepksfKRz19wGpOnnCKJW10jI7eW8EpF1pWdhTdcxZ7IGhMCFwj7ZHCmqNZLArfBI2gZYcKqR6hBDZYyzFj6SZ6J2X74JtFtIdWVasiyZ8gKviEAajZXIO2dn7cwwk17BWuFsP5NZ8l v07haNR0dcYwa9V4Nt3t8o7ZJSlXwELzODYA3WPsq4pUaof2dz8bsB1Fv2Hbe0VarRC9uqkthty1MImPBG5tDNbXZlTU4dh9Ph WIPtudfX3BRmptNHhJ5vPn2NJN41UIj70c0tgwNALFOgzk8NynQ5cGdz7CD8sQufqZPtlaDBV4ndTAgRpIg79DSA8SxN8eDQP4YrT6wDxJMxA9Aaerojes3EiQFc PVjqyqJ0oUDQvNK9rJ1ANrgJrcF jyk8BZtH Dipxg6HXKlDdLB5Tb8NObOnOBesJYHMY2iPQWKHhJc7g1hxJy9aUfdo5J4d9AyNDo83kPbNgqhsJO5tu7ZBaZVsJsV19H26SkHY8Z1vZOlQac7uKnqBZpp5OFwyHMOqIfw2Nf B6pmiF2lE1AlkMdICL2Nqh N8I54R918QZNNXDNtHnZWeLaGRqmS9DZBIwGkMm2COY3naU1IoF6yQY1MccPmebAdTNAmey1ArqvZCek5EXCJOoasrRE3qBIUSZXlU87odvxNCKJ78pZeP7U8Ed7RrnN3SbiDyEiY c7eDjdF4AAzcEr2 UlGGznQxBDriVuWBRWugpdIufzu5rk9KUe13Sa 5fPTAoHNXyjRIDObArGnjBHjPHPFM4nxyhk6mm2JCCYfNhKUmL5CBEf9jImdwRpu3KxQ1mv7bH9vKUWPcLMpVoX5P5gXvN1eOI0ZYyPoMDLd7UvcOrnjXL  2t4E0GG8TBRqLfbCLqyuBaePrnA0lIPHGQLMDoPe3IBidztyAhR KwoCWrwt2QbmvYs3KRaidfYuvMQ2 IlxUazVSZgJnc4PIpg cZkIWaTuQakpDyvJozz3yL2F4RIv14GovVvTq9QTpYkOvqHZxolngw0qpGbMeALhwFlWGpot5jgqeQjA VYA72jb2fxoWBl45AnqdW1czHYXG46kdRnUzrCenkF0mAkDuV0gRPY222BC7uWHAn6PTEWgDB3HyoBqPvanbc6s2ccdzSHJ4YJQWfAX td7UqFApODVkTbW6G7mjzuCeSpMoULyouH q1s0LjyECDXokV1Kri KhWGJUugEuxquue vh9AVw09QW fhya0F8ZmKVqD78G9EFbpMQjvOvgPlmCcvUmnxi3PXFDNkJG8WRPzocUVe3PTw0E3eEHghOKiEB4u0Xvt2Hb2esODlsJ5Uajn7B46Bq0w3W55MDUw0U5i8CP6QDrizWsQOYQOCF3vpLGOCVIyeleOWkVPz51u30XZCD7jKlRYvYOw2Rxocfq2YdbPZcvhPN7iRT ToHlNUY' AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN "" AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN '\u01FF' AS a

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN "a" AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN "🧐🍌❖⋙⚐" AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals6.feature
RETURN '\uH'

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [false] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [null] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [1] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [-0x162CD4F6] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [0o2613152366] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [-.1e-5] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN ['abc, as#?lßdj '] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [[]] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [[[[[[[]]]]]]] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [{}] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [1, -2, 0o77, 0xA4C, 71034856] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [2E-01, ', as#?lßdj ', null, 71034856, false] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [null, [ ' a ', ' ' ], ' [ a ', ' [ ], ] ', ' [ ', [ ' ' ], ' ] ' ] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [ {
            id: '0001',
            type: 'donut',
            name: 'Cake',
            ppu: 0.55,
            batters:
                {
                    batter:
                        [
                            { id: '1001', type: 'Regular' },
                            { id: '1002', type: 'Chocolate' },
                            { id: '1003', type: 'Blueberry' },
                            { id: '1004', type: 'Devils Food' }
                        ]
                },
            topping:
                [
                    { id: '5001', type: 'None' },
                    { id: '5002', type: 'Glazed' },
                    { id: '5005', type: 'Sugar' },
                    { id: '5007', type: 'Powdered Sugar' },
                    { id: '5006', type: 'Chocolate Sprinkles' },
                    { id: '5003', type: 'Chocolate' },
                    { id: '5004', type: 'Maple' }
                ]
        },
        {
            id: '0002',
            type: 'donut',
            name: 'Raised',
            ppu: 0.55,
            batters:
                {
                    batter:
                        [
                            { id: '1001', type: 'Regular' }
                        ]
                },
            topping:
                [
                    { id: '5001', type: 'None' },
                    { id: '5002', type: 'Glazed' },
                    { id: '5005', type: 'Sugar' },
                    { id: '5003', type: 'Chocolate' },
                    { id: '5004', type: 'Maple' }
                ]
        },
        {
            id: '0003',
            type: 'donut',
            name: 'Old Fashioned',
            ppu: 0.55,
            batters:
                {
                    batter:
                        [
                            { id: '1001', type: 'Regular' },
                            { id: '1002', type: 'Chocolate' }
                        ]
                },
            topping:
                [
                    { id: '5001', type: 'None' },
                    { id: '5002', type: 'Glazed' },
                    { id: '5003', type: 'Chocolate' },
                    { id: '5004', type: 'Maple' }
                ]
        } ] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [, ] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [[[]] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals7.feature
RETURN [[','[]',']] AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {abc: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {ABC: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {aBCdeF: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {a1B2c3e67: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: false} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: null} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {F: -0x162CD4F6} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: 0o2613152366} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: -.1e-5} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: 'ab: c, as#?lßdj '} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {a: {}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {a1: {a2: {a3: {a4: {a5: {a6: {}}}}}}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {a1: {a2: {a3: {a4: {a5: {a6: {a7: {a8: {a9: {a10: {a11: {a12: {a13: {a14: {a15: {a16: {a17: {a18: {a19: {}}}}}}}}}}}}}}}}}}}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {a1: {a2: {a3: {a4: {a5: {a6: {a7: {a8: {a9: {a10: {a11: {a12: {a13: {a14: {a15: {a16: {a17: {a18: {a19: {a20: {a21: {a22: {a23: {a24: {a25: {a26: {a27: {a28: {a29: {a30: {a31: {a32: {a33: {a34: {a35: {a36: {a37: {a38: {a39: {}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN { a : ' { b : ' , c : { d : ' ' } , d : ' } ' } AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN  { data: [ {
            id: '0001',
            type: 'donut',
            name: 'Cake',
            ppu: 0.55,
            batters:
                {
                    batter:
                        [
                            { id: '1001', type: 'Regular' },
                            { id: '1002', type: 'Chocolate' },
                            { id: '1003', type: 'Blueberry' },
                            { id: '1004', type: 'Devils Food' }
                        ]
                },
            topping:
                [
                    { id: '5001', type: 'None' },
                    { id: '5002', type: 'Glazed' },
                    { id: '5005', type: 'Sugar' },
                    { id: '5007', type: 'Powdered Sugar' },
                    { id: '5006', type: 'Chocolate Sprinkles' },
                    { id: '5003', type: 'Chocolate' },
                    { id: '5004', type: 'Maple' }
                ]
        },
        {
            id: '0002',
            type: 'donut',
            name: 'Raised',
            ppu: 0.55,
            batters:
                {
                    batter:
                        [
                            { id: '1001', type: 'Regular' }
                        ]
                },
            topping:
                [
                    { id: '5001', type: 'None' },
                    { id: '5002', type: 'Glazed' },
                    { id: '5005', type: 'Sugar' },
                    { id: '5003', type: 'Chocolate' },
                    { id: '5004', type: 'Maple' }
                ]
        },
        {
            id: '0003',
            type: 'donut',
            name: 'Old Fashioned',
            ppu: 0.55,
            batters:
                {
                    batter:
                        [
                            { id: '1001', type: 'Regular' },
                            { id: '1002', type: 'Chocolate' }
                        ]
                },
            topping:
                [
                    { id: '5001', type: 'None' },
                    { id: '5002', type: 'Glazed' },
                    { id: '5003', type: 'Chocolate' },
                    { id: '5004', type: 'Maple' }
                ]
        } ] } AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {1B2c3e67:1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k1#k: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k1.k: 1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k1: k2} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {, } AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {1} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {[]} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {{}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/literals/Literals8.feature
RETURN {k: {k: {}} AS literal

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {existing: 42, notMissing: null} AS m
RETURN m.missing, m.notMissing, m.existing

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH null AS m
RETURN m.missing

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH [123, {existing: 42, notMissing: null}] AS list
RETURN (list[1]).missing, (list[1]).notMissing, (list[1]).existing

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map.name AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', Name: 'Pontus'} AS map
RETURN map.name AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', Name: 'Pontus'} AS map
RETURN map.Name AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', Name: 'Pontus'} AS map
RETURN map.nAMe AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map.`name` AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map.`nome` AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map.`Mats` AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map.`null` AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {null: 'Mats', NULL: 'Pontus'} AS map
RETURN map.`null` AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH {null: 'Mats', NULL: 'Pontus'} AS map
RETURN map.`NULL` AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH 123 AS nonMap
RETURN nonMap.num

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH 42.45 AS nonMap
RETURN nonMap.num

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH true AS nonMap
RETURN nonMap.num

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH false AS nonMap
RETURN nonMap.num

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH 'string' AS nonMap
RETURN nonMap.num

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map1.feature
WITH [123, true] AS nonMap
RETURN nonMap.num

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[idx] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[toString(idx)] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH null AS expr, 'x' AS idx
RETURN expr[idx] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {name: 'Mats'} AS expr, null AS idx
RETURN expr[idx] AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map['name'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {name: 'Mats', Name: 'Pontus'} AS map
RETURN map['name'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {name: 'Mats', Name: 'Pontus'} AS map
RETURN map['Name'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {name: 'Mats', Name: 'Pontus'} AS map
RETURN map['nAMe'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {name: 'Mats', nome: 'Pontus'} AS map
RETURN map['null'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {null: 'Mats', NULL: 'Pontus'} AS map
RETURN map['null'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH {null: 'Mats', NULL: 'Pontus'} AS map
RETURN map['NULL'] AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map2.feature
WITH $expr AS expr, $idx AS idx
RETURN expr[idx]

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({name: 'Alice', age: 38, address: {city: 'London', residential: true}}) AS k

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys($param) AS k

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
WITH null AS m
RETURN keys(m), keys(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({k: 1}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({k: null}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({k: null, l: 1}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({k: 1, l: null}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({k: null, l: null}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
RETURN keys({k: 1, l: null, m: 1}) AS keys

// ../../cypher-tck/tck-M23/tck/features/expressions/map/Map3.feature
WITH {exists: 42, notMissing: null} AS map
RETURN 'exists' IN keys(map) AS a,
       'notMissing' IN keys(map) AS b,
       'missing' IN keys(map) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical11.feature
RETURN abs(-1)

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical13.feature
RETURN sqrt(12.96)

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical2.feature
MATCH (a)
WHERE a.id = 1337
RETURN a.version + 5

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical3.feature
RETURN 42 — 41

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical8.feature
RETURN 12 / 4 * 3 - 2 * 4

// ../../cypher-tck/tck-M23/tck/features/expressions/mathematical/Mathematical8.feature
RETURN 12 / 4 * (3 - 2 * 4)

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
MATCH (n)
RETURN n.missing IS NULL,
       n.exists IS NULL

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
OPTIONAL MATCH (n)
RETURN n.missing IS NULL,
       n.exists IS NULL

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
OPTIONAL MATCH (n)
RETURN n.missing IS NULL

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
RETURN null IS NULL AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: 'Mats', name2: 'Pontus'} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: 'Mats', name2: 'Pontus'} AS map
RETURN map.name2 IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: 'Mats', name2: null} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: 'Mats', name2: null} AS map
RETURN map.name2 IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: null} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: null, name2: null} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {name: null, name2: null} AS map
RETURN map.name2 IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {notName: null, notName2: null} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {notName: 0, notName2: null} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {notName: 0} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH {} AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
WITH null AS map
RETURN map.name IS NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null1.feature
MATCH (n:X)
RETURN n, n.prop iS NuLl AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
MATCH (n)
RETURN n.missing IS NOT NULL,
       n.exists IS NOT NULL

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
OPTIONAL MATCH (n)
RETURN n.missing IS NOT NULL,
       n.exists IS NOT NULL

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
OPTIONAL MATCH (n)
RETURN n.missing IS NOT NULL

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
RETURN null IS NOT NULL AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: 'Mats', name2: 'Pontus'} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: 'Mats', name2: 'Pontus'} AS map
RETURN map.name2 IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: 'Mats', name2: null} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: 'Mats', name2: null} AS map
RETURN map.name2 IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: null} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: null, name2: null} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {name: null, name2: null} AS map
RETURN map.name2 IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {notName: null, notName2: null} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {notName: 0, notName2: null} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {notName: 0} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH {} AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
WITH null AS map
RETURN map.name IS NOT NULL AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null2.feature
MATCH (n:X)
RETURN n, n.prop Is noT nULl AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN NOT null AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN null = null AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN null <> null AS value

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/null/Null3.feature
RETURN $elt IN $coll AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path1.feature
WITH null AS a
OPTIONAL MATCH p = (a)-[r]->()
RETURN nodes(p), nodes(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path2.feature
MATCH p = (a:Start)-[:REL*2..2]->(b)
RETURN relationships(p)

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path2.feature
MATCH p = (a)-[:REL*2..2]->(b:End)
RETURN relationships(p)

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path2.feature
WITH null AS a
OPTIONAL MATCH p = (a)-[r]->()
RETURN relationships(p), relationships(null)

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path3.feature
MATCH p = (a)-[*0..1]->(b)
RETURN a, b, length(p) AS l

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path3.feature
MATCH (n)
RETURN length(n)

// ../../cypher-tck/tck-M23/tck/features/expressions/path/Path3.feature
MATCH ()-[r]->()
RETURN length(r)

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[]->() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)<-[]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[:REL1]->() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[:REL1]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)<-[:REL1]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[:REL1*]->() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[:REL1*]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)<-[:REL1*]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n)-[:REL1*2]-() RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n) WHERE (n) RETURN n

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n), (m) WHERE (n)-[]->(m) RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n), (m) WHERE (n)-[:REL1]->(m) RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n), (m) WHERE (n)-[:REL1]-(m) RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n), (m) WHERE (n)-[:REL1*]->(m) RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n), (m) WHERE (n)-[:REL1*]-(m) RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern1.feature
MATCH (n), (m) WHERE (n)-[:REL1*2]-(m) RETURN n, m

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n)
RETURN [p = (n)-->() | p] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n:A)
RETURN [p = (n)-->(:B) | p] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (a:A), (b:B)
RETURN [p = (a)-->(b) | p] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n)
RETURN [(n)-[:T]->(b) | b.name] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n)
RETURN [(n)-[r:T]->() | r.name] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n:A)
RETURN count([p = (n)-[:HAS]->() | p]) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH p = (n:X)-->()
RETURN n, [x IN nodes(p) | size([(x)-->(:Y) | 1])] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n)-->(b)
WITH [p = (n)-->() | p] AS ps, count(b) AS c
RETURN ps, c

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (a:A), (b:B)
WITH [p = (a)-[*]->(b) | p] AS paths, count(a) AS c
RETURN paths, c

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (n:A)
RETURN [p = (n)-[:HAS]->() | p] AS ps

// ../../cypher-tck/tck-M23/tck/features/expressions/pattern/Pattern2.feature
MATCH (liker)
RETURN [p = (liker)--() | p] AS isNew
  ORDER BY liker.time

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN true OR true XOR true AS a,
       true OR (true XOR true) AS b,
       (true OR true) XOR true AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN true XOR false AND false AS a,
       true XOR (false AND false) AS b,
       (true XOR false) AND false AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN true OR false AND false AS a,
       true OR (false AND false) AS b,
       (true OR false) AND false AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN NOT true AND false AS a,
       (NOT true) AND false AS b,
       NOT (true AND false) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN NOT false OR true AS a,
       (NOT false) OR true AS b,
       NOT (false OR true) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN NOT false >= false AS a,
       NOT (false >= false) AS b,
       (NOT false) >= false AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN true OR false = false AS a,
       true OR (false = false) AS b,
       (true OR false) = false AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN false = true IS NULL AS a,
       false = (true IS NULL) AS b,
       (false = true) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN NOT false IS NULL AS a,
       NOT (false IS NULL) AS b,
       (NOT false) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN true OR false IS NULL AS a,
       true OR (false IS NULL) AS b,
       (true OR false) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN false = true IN [true, false] AS a,
       false = (true IN [true, false]) AS b,
       (false = true) IN [true, false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN NOT true IN [true, false] AS a,
       NOT (true IN [true, false]) AS b,
       (NOT true) IN [true, false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
RETURN false AND true IN [true, false] AS a,
       false AND (true IN [true, false]) AS b,
       (false AND true) IN [true, false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a OR b XOR c) = (a OR (b XOR c))) AS eq,
     collect((a OR b XOR c) <> ((a OR b) XOR c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a XOR b AND c) = (a XOR (b AND c))) AS eq,
     collect((a XOR b AND c) <> ((a XOR b) AND c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a OR b AND c) = (a OR (b AND c))) AS eq,
     collect((a OR b AND c) <> ((a OR b) AND c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT a AND b) = ((NOT a) AND b)) AS eq,
     collect((NOT a AND b) <> (NOT (a AND b))) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT a OR b) = ((NOT a) OR b)) AS eq,
     collect((NOT a OR b) <> (NOT (a OR b))) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT a <comp> b) = (NOT (a <comp> b))) AS eq,
     collect((NOT a <comp> b) <> ((NOT a) <comp> b)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT (a = b)) = ((NOT a) = b)) AS eq
RETURN all(x IN eq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT (a <> b)) = ((NOT a) <> b)) AS eq
RETURN all(x IN eq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a OR b = c) = (a OR (b = c))) AS eq,
     collect((a OR b = c) <> ((a OR b) = c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a XOR (b = c)) = ((a XOR b) = c)) AS eq
RETURN all(x IN eq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a OR (b >= c)) = ((a OR b) >= c)) AS eq
RETURN all(x IN eq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a AND (b > c)) = ((a AND b) > c)) AS eq
RETURN all(x IN eq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [true, false, null] AS c
WITH collect((a XOR (b <> c)) = ((a XOR b) <> c)) AS eq
RETURN all(x IN eq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a = b IS NULL) = (a = (b IS NULL))) AS eq,
     collect((a = b IS NULL) <> ((a = b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a = b IS NOT NULL) = (a = (b IS NOT NULL))) AS eq,
     collect((a = b IS NOT NULL) <> ((a = b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a <= b IS NULL) = (a <= (b IS NULL))) AS eq,
     collect((a <= b IS NULL) <> ((a <= b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a <= b IS NOT NULL) = (a <= (b IS NOT NULL))) AS eq,
     collect((a <= b IS NOT NULL) <> ((a <= b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a >= b IS NULL) = (a >= (b IS NULL))) AS eq,
     collect((a >= b IS NULL) <> ((a >= b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a >= b IS NOT NULL) = (a >= (b IS NOT NULL))) AS eq,
     collect((a >= b IS NOT NULL) <> ((a >= b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a < b IS NULL) = (a < (b IS NULL))) AS eq,
     collect((a < b IS NULL) <> ((a < b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a < b IS NOT NULL) = (a < (b IS NOT NULL))) AS eq,
     collect((a < b IS NOT NULL) <> ((a < b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a > b IS NULL) = (a > (b IS NULL))) AS eq,
     collect((a > b IS NULL) <> ((a > b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a > b IS NOT NULL) = (a > (b IS NOT NULL))) AS eq,
     collect((a > b IS NOT NULL) <> ((a > b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a <> b IS NULL) = (a <> (b IS NULL))) AS eq,
     collect((a <> b IS NULL) <> ((a <> b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a <> b IS NOT NULL) = (a <> (b IS NOT NULL))) AS eq,
     collect((a <> b IS NOT NULL) <> ((a <> b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT a IS NULL) = (NOT (a IS NULL))) AS eq,
     collect((NOT a IS NULL) <> ((NOT a) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((NOT a IS NOT NULL) = (NOT (a IS NOT NULL))) AS eq,
     collect((NOT a IS NOT NULL) <> ((NOT a) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a OR b IS NULL) = (a OR (b IS NULL))) AS eq,
     collect((a OR b IS NULL) <> ((a OR b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a OR b IS NOT NULL) = (a OR (b IS NOT NULL))) AS eq,
     collect((a OR b IS NOT NULL) <> ((a OR b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a XOR b IS NULL) = (a XOR (b IS NULL))) AS eq,
     collect((a XOR b IS NULL) <> ((a XOR b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a XOR b IS NOT NULL) = (a XOR (b IS NOT NULL))) AS eq,
     collect((a XOR b IS NOT NULL) <> ((a XOR b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a AND b IS NULL) = (a AND (b IS NULL))) AS eq,
     collect((a AND b IS NULL) <> ((a AND b) IS NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
WITH collect((a AND b IS NOT NULL) = (a AND (b IS NOT NULL))) AS eq,
     collect((a AND b IS NOT NULL) <> ((a AND b) IS NOT NULL)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a = b IN c) = (a = (b IN c))) AS eq,
     collect((a = b IN c) <> ((a = b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a <= b IN c) = (a <= (b IN c))) AS eq,
     collect((a <= b IN c) <> ((a <= b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a >= b IN c) = (a >= (b IN c))) AS eq,
     collect((a >= b IN c) <> ((a >= b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a < b IN c) = (a < (b IN c))) AS eq,
     collect((a < b IN c) <> ((a < b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a > b IN c) = (a > (b IN c))) AS eq,
     collect((a > b IN c) <> ((a > b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a <> b IN c) = (a <> (b IN c))) AS eq,
     collect((a <> b IN c) <> ((a <> b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS b
WITH collect((NOT a IN b) = (NOT (a IN b))) AS eq,
     collect((NOT a IN b) <> ((NOT a) IN b)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a OR b IN c) = (a OR (b IN c))) AS eq,
     collect((a OR b IN c) <> ((a OR b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a XOR b IN c) = (a XOR (b IN c))) AS eq,
     collect((a XOR b IN c) <> ((a XOR b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence1.feature
UNWIND [true, false, null] AS a
UNWIND [true, false, null] AS b
UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
WITH collect((a AND b IN c) = (a AND (b IN c))) AS eq,
     collect((a AND b IN c) <> ((a AND b) IN c)) AS neq
RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 * 2 + 3 * 2 AS a,
       4 * 2 + (3 * 2) AS b,
       4 * (2 + 3) * 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 * 2 + 3 / 2 AS a,
       4 * 2 + (3 / 2) AS b,
       4 * (2 + 3) / 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 * 2 + 3 % 2 AS a,
       4 * 2 + (3 % 2) AS b,
       4 * (2 + 3) % 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 * 2 - 3 * 2 AS a,
       4 * 2 - (3 * 2) AS b,
       4 * (2 - 3) * 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 * 2 - 3 / 2 AS a,
       4 * 2 - (3 / 2) AS b,
       4 * (2 - 3) / 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 * 2 - 3 % 2 AS a,
       4 * 2 - (3 % 2) AS b,
       4 * (2 - 3) % 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 / 2 + 3 * 2 AS a,
       4 / 2 + (3 * 2) AS b,
       4 / (2 + 3) * 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 / 2 + 3 / 2 AS a,
       4 / 2 + (3 / 2) AS b,
       4 / (2 + 3) / 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 / 2 + 3 % 2 AS a,
       4 / 2 + (3 % 2) AS b,
       4 / (2 + 3) % 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 / 2 - 3 * 2 AS a,
       4 / 2 - (3 * 2) AS b,
       4 / (2 - 3) * 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 / 2 - 3 / 2 AS a,
       4 / 2 - (3 / 2) AS b,
       4 / (2 - 3) / 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 / 2 - 3 % 2 AS a,
       4 / 2 - (3 % 2) AS b,
       4 / (2 - 3) % 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 % 2 + 3 * 2 AS a,
       4 % 2 + (3 * 2) AS b,
       4 % (2 + 3) * 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 % 2 + 3 / 2 AS a,
       4 % 2 + (3 / 2) AS b,
       4 % (2 + 3) / 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 % 2 + 3 % 2 AS a,
       4 % 2 + (3 % 2) AS b,
       4 % (2 + 3) % 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 % 2 - 3 * 2 AS a,
       4 % 2 - (3 * 2) AS b,
       4 % (2 - 3) * 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 % 2 - 3 / 2 AS a,
       4 % 2 - (3 / 2) AS b,
       4 % (2 - 3) / 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 % 2 - 3 % 2 AS a,
       4 % 2 - (3 % 2) AS b,
       4 % (2 - 3) % 2 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 ^ 3 * 2 ^ 3 AS a,
       (4 ^ 3) * (2 ^ 3) AS b,
       4 ^ (3 * 2) ^ 3 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 ^ 3 / 2 ^ 3 AS a,
       (4 ^ 3) / (2 ^ 3) AS b,
       4 ^ (3 / 2) ^ 3 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 ^ 3 % 2 ^ 3 AS a,
       (4 ^ 3) % (2 ^ 3) AS b,
       4 ^ (3 % 2) ^ 3 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 ^ 3 + 2 ^ 3 AS a,
       (4 ^ 3) + (2 ^ 3) AS b,
       4 ^ (3 + 2) ^ 3 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN 4 ^ 3 - 2 ^ 3 AS a,
       (4 ^ 3) - (2 ^ 3) AS b,
       4 ^ (3 - 2) ^ 3 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN -3 ^ 2 AS a,
       (-3) ^ 2 AS b,
       -(3 ^ 2) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN -3 + 2 AS a,
       (-3) + 2 AS b,
       -(3 + 2) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence2.feature
RETURN -3 - 2 AS a,
       (-3) - 2 AS b,
       -(3 - 2) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10][3] AS a,
       [[1], [2, 3], [4, 5]] + ([5, [6, 7], [8, 9], 10][3]) AS b,
       ([[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10])[3] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10][2] AS a,
       [[1], [2, 3], [4, 5]] + ([5, [6, 7], [8, 9], 10][2]) AS b,
       ([[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10])[2] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10][1..3] AS a,
       [[1], [2, 3], [4, 5]] + ([5, [6, 7], [8, 9], 10][1..3]) AS b,
       ([[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10])[1..3] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1]+2 IN [3]+4 AS a,
       ([1]+2) IN ([3]+4) AS b,
       [1]+(2 IN [3])+4 AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1]+[2] IN [3]+[4] AS a,
       ([1]+[2]) IN ([3]+[4]) AS b,
       (([1]+[2]) IN [3])+[4] AS c,
       [1]+([2] IN [3])+[4] AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1, 2] = [3, 4] IN [[3, 4], false] AS a,
       [1, 2] = ([3, 4] IN [[3, 4], false]) AS b,
       ([1, 2] = [3, 4]) IN [[3, 4], false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1, 2] <> [3, 4] IN [[3, 4], false] AS a,
       [1, 2] <> ([3, 4] IN [[3, 4], false]) AS b,
       ([1, 2] <> [3, 4]) IN [[3, 4], false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1, 2] < [3, 4] IN [[3, 4], false] AS a,
       [1, 2] < ([3, 4] IN [[3, 4], false]) AS b,
       ([1, 2] < [3, 4]) IN [[3, 4], false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1, 2] > [3, 4] IN [[3, 4], false] AS a,
       [1, 2] > ([3, 4] IN [[3, 4], false]) AS b,
       ([1, 2] > [3, 4]) IN [[3, 4], false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1, 2] <= [3, 4] IN [[3, 4], false] AS a,
       [1, 2] <= ([3, 4] IN [[3, 4], false]) AS b,
       ([1, 2] <= [3, 4]) IN [[3, 4], false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence3.feature
RETURN [1, 2] >= [3, 4] IN [[3, 4], false] AS a,
       [1, 2] >= ([3, 4] IN [[3, 4], false]) AS b,
       ([1, 2] >= [3, 4]) IN [[3, 4], false] AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN null IS NOT NULL = null IS NULL AS a,
       (null IS NOT NULL) = (null IS NULL) AS b,
       (null IS NOT NULL = null) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN null IS NULL <> null IS NULL AS a,
       (null IS NULL) <> (null IS NULL) AS b,
       (null IS NULL <> null) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN null IS NULL <> null IS NOT NULL AS a,
       (null IS NULL) <> (null IS NOT NULL) AS b,
       (null IS NULL <> null) IS NOT NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN NOT null IS NULL AS a,
       NOT (null IS NULL) AS b,
       (NOT null) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN null AND null IS NULL AS a,
       null AND (null IS NULL) AS b,
       (null AND null) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN null AND true IS NULL AS a,
       null AND (true IS NULL) AS b,
       (null AND true) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN false AND false IS NOT NULL AS a,
       false AND (false IS NOT NULL) AS b,
       (false AND false) IS NOT NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN null OR false IS NULL AS a,
       null OR (false IS NULL) AS b,
       (null OR false) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN true OR null IS NULL AS a,
       true OR (null IS NULL) AS b,
       (true OR null) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN true XOR null IS NOT NULL AS a,
       true XOR (null IS NOT NULL) AS b,
       (true XOR null) IS NOT NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN true XOR false IS NULL AS a,
       true XOR (false IS NULL) AS b,
       (true XOR false) IS NULL AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/precedence/Precedence4.feature
RETURN ('abc' STARTS WITH null OR true) = (('abc' STARTS WITH null) OR true) AS a,
       ('abc' STARTS WITH null OR true) <> ('abc' STARTS WITH (null OR true)) AS b,
       (true OR null STARTS WITH 'abc') = (true OR (null STARTS WITH 'abc')) AS c,
       (true OR null STARTS WITH 'abc') <> ((true OR null) STARTS WITH 'abc') AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE true) AS a, none(x IN [] WHERE false) AS b, none(x IN [] WHERE x) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [true, false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [false, true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [true, true, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [false, false, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1, 3, 20, 5000] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [20, 3, 5000, -2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [3, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, 3, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, -10, 3, 9, 0] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, -10, 3, 2, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, -10, 3, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [200, -10, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [200, 15, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1.1, 3.5, 20.0, 50.42435] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [20.0, 3.4, 50.2, -2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1.43, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1.43, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2.1, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [3.5, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2.1, 3.5, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['abc', 'ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['ef', 'abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['abc', 'abc', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['ef', 'ef', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [[1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [[1, 2, 3], ['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [['a'], [1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [[1, 2, 3], [1, 2, 3], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [['a'], ['a'], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 2, b: 5}, {a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 4}, {a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 2, b: 5}, {a: 2, b: 5}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [{a: 4}, {a: 4}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
MATCH p = (:SNodes)-[*0..3]->(x)
WITH tail(nodes(p)) AS nodes
RETURN nodes, none(x IN nodes WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
MATCH p = (:SRelationships)-[*0..4]->(x)
WITH tail(relationships(p)) AS relationships, COUNT(*) AS c
RETURN relationships, none(x IN relationships WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [0, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, null, 5, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 10, null, 15, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [4, 0, null, -15, 9] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [0] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [0, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, 2] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, null, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, null, 8, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, 123, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, null, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [0] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [0, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [2, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, 2] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, null, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [34, 0, null, 8, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, 123, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [null, null, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1, null, true, 4.5, 'abc', false] WHERE false) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [1, null, true, 4.5, 'abc', false] WHERE true) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['Clara'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN [false, true] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier1.feature
RETURN none(x IN ['Clara', 'Bob', 'Dave', 'Alice'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH single(x IN list WHERE false) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 1
WITH single(x IN list WHERE true) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
WITH [1, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS element
WITH single(x IN [element] WHERE true) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH single(x IN list WHERE x = 2) = (size([x IN list WHERE x = 2 | x]) = 1) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH single(x IN list WHERE x % 2 = 0) = (size([x IN list WHERE x % 2 = 0 | x]) = 1) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH single(x IN list WHERE x % 3 = 0) = (size([x IN list WHERE x % 3 = 0 | x]) = 1) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH single(x IN list WHERE x < 7) = (size([x IN list WHERE x < 7 | x]) = 1) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier10.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH single(x IN list WHERE x >= 3) = (size([x IN list WHERE x >= 3 | x]) = 1) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH any(x IN list WHERE false) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH any(x IN list WHERE true) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH list WHERE single(x IN list WHERE x = 2) OR all(x IN list WHERE x = 2)
WITH any(x IN list WHERE x = 2) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH list WHERE single(x IN list WHERE x % 2 = 0) OR all(x IN list WHERE x % 2 = 0)
WITH any(x IN list WHERE x % 2 = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH list WHERE single(x IN list WHERE x % 3 = 0) OR all(x IN list WHERE x % 3 = 0)
WITH any(x IN list WHERE x % 3 = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH list WHERE single(x IN list WHERE x < 7) OR all(x IN list WHERE x < 7)
WITH any(x IN list WHERE x < 7) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH list WHERE single(x IN list WHERE x >= 3) OR all(x IN list WHERE x >= 3)
WITH any(x IN list WHERE x >= 3) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x = 2) = (NOT none(x IN list WHERE x = 2)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x % 2 = 0) = (NOT none(x IN list WHERE x % 2 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x % 3 = 0) = (NOT none(x IN list WHERE x % 3 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x < 7) = (NOT none(x IN list WHERE x < 7)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x >= 3) = (NOT none(x IN list WHERE x >= 3)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x = 2) = (NOT all(x IN list WHERE NOT (x = 2))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x % 2 = 0) = (NOT all(x IN list WHERE NOT (x % 2 = 0))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x % 3 = 0) = (NOT all(x IN list WHERE NOT (x % 3 = 0))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x < 7) = (NOT all(x IN list WHERE NOT (x < 7))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH any(x IN list WHERE x >= 3) = (NOT all(x IN list WHERE NOT (x >= 3))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH any(x IN list WHERE x = 2) = (size([x IN list WHERE x = 2 | x]) > 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH any(x IN list WHERE x % 2 = 0) = (size([x IN list WHERE x % 2 = 0 | x]) > 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH any(x IN list WHERE x % 3 = 0) = (size([x IN list WHERE x % 3 = 0 | x]) > 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH any(x IN list WHERE x < 7) = (size([x IN list WHERE x < 7 | x]) > 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier11.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH any(x IN list WHERE x >= 3) = (size([x IN list WHERE x >= 3 | x]) > 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH all(x IN list WHERE false) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH all(x IN list WHERE true) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x = 2) = none(x IN list WHERE NOT (x = 2)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x % 2 = 0) = none(x IN list WHERE NOT (x % 2 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x % 3 = 0) = none(x IN list WHERE NOT (x % 3 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x < 7) = none(x IN list WHERE NOT (x < 7)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x >= 3) = none(x IN list WHERE NOT (x >= 3)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x = 2) = (NOT any(x IN list WHERE NOT (x = 2))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x % 2 = 0) = (NOT any(x IN list WHERE NOT (x % 2 = 0))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x % 3 = 0) = (NOT any(x IN list WHERE NOT (x % 3 = 0))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x < 7) = (NOT any(x IN list WHERE NOT (x < 7))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH all(x IN list WHERE x >= 3) = (NOT any(x IN list WHERE NOT (x >= 3))) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH all(x IN list WHERE x = 2) = (size([x IN list WHERE x = 2 | x]) = size(list)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH all(x IN list WHERE x % 2 = 0) = (size([x IN list WHERE x % 2 = 0 | x]) = size(list)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH all(x IN list WHERE x % 3 = 0) = (size([x IN list WHERE x % 3 = 0 | x]) = size(list)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH all(x IN list WHERE x < 7) = (size([x IN list WHERE x < 7 | x]) = size(list)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier12.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH all(x IN list WHERE x >= 3) = (size([x IN list WHERE x >= 3 | x]) = size(list)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE true) AS a, single(x IN [] WHERE false) AS b, single(x IN [] WHERE x) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [true, false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [false, true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [true, true, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [false, false, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1, 3, 20, 5000] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [20, 3, 5000, -2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [3, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, 3, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, -10, 3, 9, 0] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, -10, 3, 2, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, -10, 3, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [200, -10, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [200, 15, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1.1, 3.5, 20.0, 50.42435] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [20.0, 3.4, 50.2, -2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1.43, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1.43, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2.1, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [3.5, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2.1, 3.5, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['abc', 'ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['ef', 'abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['abc', 'abc', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['ef', 'ef', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [[1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [[1, 2, 3], ['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [['a'], [1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [[1, 2, 3], [1, 2, 3], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [['a'], ['a'], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 2, b: 5}, {a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 4}, {a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 2, b: 5}, {a: 2, b: 5}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [{a: 4}, {a: 4}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
MATCH p = (:SNodes)-[*0..3]->(x)
WITH tail(nodes(p)) AS nodes
RETURN nodes, single(x IN nodes WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
MATCH p = (:SRelationships)-[*0..4]->(x)
WITH tail(relationships(p)) AS relationships, COUNT(*) AS c
RETURN relationships, single(x IN relationships WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [0, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, null, 5, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 10, null, 15, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [4, 0, null, -15, 9] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [0] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [0, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, 2] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, null, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, null, 8, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, 123, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, null, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [0] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [0, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [2, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, 2] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, null, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [34, 0, null, 8, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, 123, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [null, null, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1, null, true, 4.5, 'abc', false] WHERE false) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1, null, true, 4.5, 'abc', false] WHERE true) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [1] WHERE true) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['Clara'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN [false, true] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier2.feature
RETURN single(x IN ['Clara', 'Bob', 'Dave', 'Alice'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE true) AS a, any(x IN [] WHERE false) AS b, any(x IN [] WHERE x) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [true, false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [false, true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [true, true, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [false, false, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1, 3, 20, 5000] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [20, 3, 5000, -2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [3, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, 3, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, -10, 3, 9, 0] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, -10, 3, 2, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, -10, 3, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [200, -10, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [200, 15, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1.1, 3.5, 20.0, 50.42435] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [20.0, 3.4, 50.2, -2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1.43, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1.43, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2.1, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [3.5, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2.1, 3.5, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['abc', 'ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['ef', 'abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['abc', 'abc', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['ef', 'ef', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [[1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [[1, 2, 3], ['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [['a'], [1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [[1, 2, 3], [1, 2, 3], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [['a'], ['a'], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 2, b: 5}, {a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 4}, {a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 2, b: 5}, {a: 2, b: 5}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [{a: 4}, {a: 4}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
MATCH p = (:SNodes)-[*0..3]->(x)
WITH tail(nodes(p)) AS nodes
RETURN nodes, any(x IN nodes WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
MATCH p = (:SRelationships)-[*0..4]->(x)
WITH tail(relationships(p)) AS relationships, COUNT(*) AS c
RETURN relationships, any(x IN relationships WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [0, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, null, 5, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 10, null, 15, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [4, 0, null, -15, 9] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [0] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [0, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, 2] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, null, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, null, 8, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, 123, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, null, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [0] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [0, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [2, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, 2] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, null, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [34, 0, null, 8, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, 123, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [null, null, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1, null, true, 4.5, 'abc', false] WHERE false) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [1, null, true, 4.5, 'abc', false] WHERE true) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['Clara'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN [false, true] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier3.feature
RETURN any(x IN ['Clara', 'Bob', 'Dave', 'Alice'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE true) AS a, all(x IN [] WHERE false) AS b, all(x IN [] WHERE x) AS c

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [true, false, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [false, true, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [true, true, true] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [false, false, false] WHERE x) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1, 3, 20, 5000] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [20, 3, 5000, -2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [3, 2, 3] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, 3, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, -10, 3, 9, 0] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, -10, 3, 2, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, -10, 3, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [200, -10, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [200, 15, 36, 21, 10] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1.1, 3.5, 20.0, 50.42435] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [20.0, 3.4, 50.2, -2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1.43, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1.43, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2.1, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [3.5, 2.1, 3.5] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2.1, 3.5, 2.1] WHERE x = 2.1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['abc', 'ef', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['ef', 'abc', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['abc', 'abc', 'abc'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['ef', 'ef', 'ef'] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [[1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [[1, 2, 3], ['a'], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [['a'], [1, 2, 3], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [[1, 2, 3], [1, 2, 3], [1, 2, 3]] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [['a'], ['a'], ['a']] WHERE size(x) = 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 2, b: 5}, {a: 4}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 4}, {a: 2, b: 5}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 2, b: 5}, {a: 2, b: 5}, {a: 2, b: 5}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [{a: 4}, {a: 4}, {a: 4}] WHERE x.a = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
MATCH p = (:SNodes)-[*0..3]->(x)
WITH tail(nodes(p)) AS nodes
RETURN nodes, all(x IN nodes WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
MATCH p = (:SRelationships)-[*0..4]->(x)
WITH tail(relationships(p)) AS relationships, COUNT(*) AS c
RETURN relationships, all(x IN relationships WHERE x.name = 'a') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [0, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, null] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, 2] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, null, 5, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 10, null, 15, 900] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [4, 0, null, -15, 9] WHERE x < 10) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [0] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [0, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, 2] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, null, 8, 900] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, null, 8, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, 123, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, null, null, null] WHERE x IS NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [0] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [0, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [2, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, 2] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, null, 8, 900] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [34, 0, null, 8, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, 123, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [null, null, null, null] WHERE x IS NOT NULL) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1, null, true, 4.5, 'abc', false] WHERE false) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [1, null, true, 4.5, 'abc', false] WHERE true) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['Clara'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN [false, true] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier4.feature
RETURN all(x IN ['Clara', 'Bob', 'Dave', 'Alice'] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE none(y IN list WHERE x <= y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE none(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE single(y IN list WHERE abs(x - y) < 3)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE single(y IN list WHERE x + y = 15)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE any(y IN list WHERE x + y < 2)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE any(y IN list WHERE x + y <= 3)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE all(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN none(x IN list WHERE all(y IN list WHERE x <= y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x = 2)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 2 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 3 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x < 7)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x >= 3)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2 | x]) = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0 | x]) = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0 | x]) = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7 | x]) = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier5.feature
RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3 | x]) = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE none(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE none(y IN list WHERE x % y = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE single(y IN list WHERE x + y < 5)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE single(y IN list WHERE x % y = 1)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE any(y IN list WHERE 2 * x + y > 25)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE any(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE all(y IN list WHERE x <= y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN single(x IN list WHERE all(y IN list WHERE x <= y + 1)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2 | x]) = 1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0 | x]) = 1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0 | x]) = 1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7 | x]) = 1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier6.feature
RETURN single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3 | x]) = 1) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE none(y IN list WHERE x = y * y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE none(y IN list WHERE x % y = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE single(y IN list WHERE x = y * y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE single(y IN list WHERE x < y * y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE any(y IN list WHERE x = y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE any(y IN list WHERE x = 10 * y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE all(y IN list WHERE x <= y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN any(x IN list WHERE all(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN (single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) OR all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2)) <= any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN (single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) OR all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0)) <= any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN (single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) OR all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0)) <= any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN (single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) OR all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7)) <= any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN (single(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) OR all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3)) <= any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (NOT none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (NOT none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (NOT none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (NOT none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (NOT none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (NOT all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x = 2))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (NOT all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 2 = 0))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (NOT all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 3 = 0))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (NOT all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x < 7))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (NOT all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x >= 3))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2 | x]) > 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0 | x]) > 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0 | x]) > 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7 | x]) > 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier7.feature
RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3 | x]) > 0) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'def')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE single(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE any(y IN x WHERE y = 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y <> 'ghi')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [['abc'], ['abc', 'def']] WHERE all(y IN x WHERE y = 'abc')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE none(y IN list WHERE x = 10 * y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE none(y IN list WHERE x = y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE single(y IN list WHERE x = y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE single(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE any(y IN list WHERE x % y = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE any(y IN list WHERE x < y)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE all(y IN list WHERE abs(x - y) < 10)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
RETURN all(x IN list WHERE all(y IN list WHERE x < y + 7)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x = 2)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 2 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 3 = 0)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x < 7)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x >= 3)) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x = 2))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 2 = 0))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x % 3 = 0))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x < 7))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (x >= 3))) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2 | x]) = size([1, 2, 3, 4, 5, 6, 7, 8, 9])) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0 | x]) = size([1, 2, 3, 4, 5, 6, 7, 8, 9])) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0 | x]) = size([1, 2, 3, 4, 5, 6, 7, 8, 9])) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7 | x]) = size([1, 2, 3, 4, 5, 6, 7, 8, 9])) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier8.feature
RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3 | x]) = size([1, 2, 3, 4, 5, 6, 7, 8, 9])) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH none(x IN list WHERE false) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, null, true, 4.5, 'abc', false, '', [234, false], {a: null, b: true, c: 15.2}, {}, [], [null], [[{b: [null]}]]] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH list WHERE size(list) > 0
WITH none(x IN list WHERE true) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x = 2) = (NOT any(x IN list WHERE x = 2)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x % 2 = 0) = (NOT any(x IN list WHERE x % 2 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x % 3 = 0) = (NOT any(x IN list WHERE x % 3 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x < 7) = (NOT any(x IN list WHERE x < 7)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x >= 3) = (NOT any(x IN list WHERE x >= 3)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x = 2) = all(x IN list WHERE NOT (x = 2)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x % 2 = 0) = all(x IN list WHERE NOT (x % 2 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x % 3 = 0) = all(x IN list WHERE NOT (x % 3 = 0)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x < 7) = all(x IN list WHERE NOT (x < 7)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS inputList
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH none(x IN list WHERE x >= 3) = all(x IN list WHERE NOT (x >= 3)) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH none(x IN list WHERE x = 2) = (size([x IN list WHERE x = 2 | x]) = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH none(x IN list WHERE x % 2 = 0) = (size([x IN list WHERE x % 2 = 0 | x]) = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH none(x IN list WHERE x % 3 = 0) = (size([x IN list WHERE x % 3 = 0 | x]) = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH none(x IN list WHERE x < 7) = (size([x IN list WHERE x < 7 | x]) = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/quantifier/Quantifier9.feature
UNWIND [{list: [2], fixed: true},
        {list: [6], fixed: true},
        {list: [7], fixed: true},
        {list: [1, 2, 3, 4, 5, 6, 7, 8, 9], fixed: false}] AS input
WITH CASE WHEN input.fixed THEN input.list ELSE null END AS fixedList,
     CASE WHEN NOT input.fixed THEN input.list ELSE [1] END AS inputList
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
UNWIND inputList AS x
WITH fixedList, inputList, x, [ y IN inputList WHERE rand() > 0.5 | y] AS list
WITH fixedList, inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
WITH coalesce(fixedList, list) AS list
WITH none(x IN list WHERE x >= 3) = (size([x IN list WHERE x >= 3 | x]) = 0) AS result, count(*) AS cnt
RETURN result

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String1.feature
RETURN substring('0123456789', 1) AS s

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE a.name CONTAINS 'ABCDEF'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE a.name CONTAINS 'CD'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE a.name CONTAINS ''
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE a.name CONTAINS ' '
RETURN a.name AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE a.name CONTAINS '\n'
RETURN a.name AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE a.name CONTAINS null
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE NOT a.name CONTAINS null
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
WITH [1, 3.14, true, [], {}, null] AS operands
UNWIND operands AS op1
UNWIND operands AS op2
WITH op1 CONTAINS op2 AS v
RETURN v, count(*)

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String10.feature
MATCH (a)
WHERE NOT a.name CONTAINS 'b'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String11.feature
MATCH (a)
WHERE a.name STARTS WITH 'a'
  AND a.name ENDS WITH 'f'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String11.feature
MATCH (a)
WHERE a.name STARTS WITH 'A'
  AND a.name CONTAINS 'C'
  AND a.name ENDS WITH 'EF'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String3.feature
RETURN reverse('raksO')

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String4.feature
UNWIND split('one1two', '1') AS item
RETURN count(item) AS item

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE a.name STARTS WITH 'ABCDEF'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE a.name STARTS WITH 'ABC'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE a.name STARTS WITH ''
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE a.name STARTS WITH ' '
RETURN a.name AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE a.name STARTS WITH '\n'
RETURN a.name AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE a.name STARTS WITH null
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE NOT a.name STARTS WITH null
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
WITH [1, 3.14, true, [], {}, null] AS operands
UNWIND operands AS op1
UNWIND operands AS op2
WITH op1 STARTS WITH op2 AS v
RETURN v, count(*)

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String8.feature
MATCH (a)
WHERE NOT a.name STARTS WITH 'ab'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE a.name ENDS WITH 'AB'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE a.name ENDS WITH 'DEF'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE a.name ENDS WITH ''
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE a.name ENDS WITH ' '
RETURN a.name AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE a.name ENDS WITH '\n'
RETURN a.name AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE a.name ENDS WITH null
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE NOT a.name ENDS WITH null
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
WITH [1, 3.14, true, [], {}, null] AS operands
UNWIND operands AS op1
UNWIND operands AS op2
WITH op1 ENDS WITH op2 AS v
RETURN v, count(*)

// ../../cypher-tck/tck-M23/tck/features/expressions/string/String9.feature
MATCH (a)
WHERE NOT a.name ENDS WITH 'def'
RETURN a

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1816, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1816, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1817, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1817, week: 10}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1817, week: 30}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1817, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1818, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1818, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1818, week: 53}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1819, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1819, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({dayOfWeek: 2, year: 1817, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({date: date('1816-12-30'), week: 2, dayOfWeek: 3}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({date: date('1816-12-31'), week: 2}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({date: date('1816-12-31'), year: 1817, week: 2}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1816, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1816, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1817, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1817, week: 10}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1817, week: 30}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1817, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1818, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1818, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1818, week: 53}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1819, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1819, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({dayOfWeek: 2, year: 1817, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({date: date('1816-12-30'), week: 2, dayOfWeek: 3}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({date: date('1816-12-31'), week: 2}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({date: date('1816-12-31'), year: 1817, week: 2}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1816, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1816, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1817, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1817, week: 10}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1817, week: 30}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1817, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1818, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1818, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1818, week: 53}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1819, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1819, week: 52}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({dayOfWeek: 2, year: 1817, week: 1}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({date: date('1816-12-30'), week: 2, dayOfWeek: 3}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({date: date('1816-12-31'), week: 2}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({date: date('1816-12-31'), year: 1817, week: 2}) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, month: 10, day: 11}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, month: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, week: 10, dayOfWeek: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, week: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, ordinalDay: 202}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, quarter: 3, dayOfQuarter: 45}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN date({year: 1984, quarter: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12, minute: 31, second: 14, nanosecond: 789, millisecond: 123, microsecond: 456}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localtime({hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, nanosecond: 789, millisecond: 123, microsecond: 456}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, nanosecond: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, millisecond: 645, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 31, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 789, millisecond: 123, microsecond: 456}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, month: 10, day: 11}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, week: 10, dayOfWeek: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, ordinalDay: 202}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984, quarter: 3, dayOfQuarter: 45}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN localdatetime({year: 1984}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 789, millisecond: 123, microsecond: 456}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, microsecond: 645876}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, millisecond: 645}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, week: 10, dayOfWeek: 3, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, second: 14, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, minute: 31, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, hour: 12, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, ordinalDay: 202, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, millisecond: 645, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, second: 14, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, minute: 31, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, hour: 12, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, quarter: 3, dayOfQuarter: 45, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime.fromepoch(416779, 999999999) AS d1,
       datetime.fromepochmillis(237821673987) AS d2

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({days: 14, hours: 16, minutes: 12}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({months: 5, days: 1.5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({months: 0.75}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({weeks: 2.5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({days: 14, seconds: 70, milliseconds: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({days: 14, seconds: 70, microseconds: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({days: 14, seconds: 70, nanoseconds: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN duration({minutes: 1.5, seconds: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 34, second: 56, timezone: '+02:05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 34, second: 56, timezone: '+02:05:59'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN time({hour: 12, minute: 34, second: 56, timezone: '-02:05:07'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal1.feature
RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 34, second: 56, timezone: '+02:05:59'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
WITH duration.between(localdatetime('2018-01-01T12:00'), localdatetime('2018-01-02T10:00')) AS dur
RETURN dur, dur.days, dur.seconds, dur.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
WITH duration.between(localdatetime('2018-01-02T10:00'), localdatetime('2018-01-01T12:00')) AS dur
RETURN dur, dur.days, dur.seconds, dur.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
WITH duration.between(localdatetime('2018-01-01T10:00:00.2'), localdatetime('2018-01-02T10:00:00.1')) AS dur
RETURN dur, dur.days, dur.seconds, dur.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
WITH duration.between(localdatetime('2018-01-02T10:00:00.1'), localdatetime('2018-01-01T10:00:00.2')) AS dur
RETURN dur, dur.days, dur.seconds, dur.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
WITH duration.between(datetime('2017-10-28T23:00+02:00[Europe/Stockholm]'), datetime('2017-10-29T04:00+01:00[Europe/Stockholm]')) AS dur
RETURN dur, dur.days, dur.seconds, dur.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
WITH duration.between(datetime('2017-10-29T04:00+01:00[Europe/Stockholm]'), datetime('2017-10-28T23:00+02:00[Europe/Stockholm]')) AS dur
RETURN dur, dur.days, dur.seconds, dur.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(date('1984-10-11'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(date('1984-10-11'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(date('1984-10-11'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(date('1984-10-11'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localtime('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localtime('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localtime('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localtime('14:30'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localtime('14:30'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(time('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(time('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(time('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(time('14:30'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(time('14:30'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(datetime('2014-07-21T21:40:36.143+0200'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(datetime('2014-07-21T21:40:36.143+0200'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(datetime('2014-07-21T21:40:36.143+0200'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(datetime('2014-07-21T21:40:36.143+0200'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(datetime('2014-07-21T21:40:36.143+0200'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('1984-10-11'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('1984-10-11'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('1984-10-11'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('1984-10-11'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('1984-10-11'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localtime('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localtime('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localtime('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(time('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(time('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(time('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localdatetime('2015-07-21T21:40:32.142'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localdatetime('2015-07-21T21:40:32.142'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localdatetime('2015-07-21T21:40:32.142'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localdatetime('2015-07-21T21:40:32.142'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2014-07-21T21:40:36.143+0200'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2014-07-21T21:40:36.143+0200'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2014-07-21T21:40:36.143+0200'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2014-07-21T21:40:36.143+0200'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2014-07-21T21:40:36.143+0200'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(date('1984-10-11'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(date('1984-10-11'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(date('1984-10-11'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(date('1984-10-11'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(date('1984-10-11'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localtime('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localtime('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localtime('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(time('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(time('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(time('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localdatetime('2015-07-21T21:40:32.142'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localdatetime('2015-07-21T21:40:32.142'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localdatetime('2015-07-21T21:40:32.142'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(localdatetime('2015-07-21T21:40:32.142'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(datetime('2014-07-21T21:40:36.143+0200'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(datetime('2014-07-21T21:40:36.143+0200'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(datetime('2014-07-21T21:40:36.143+0200'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(datetime('2014-07-21T21:40:36.143+0200'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(datetime('2014-07-21T21:40:36.143+0200'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date('1984-10-11'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date('1984-10-11'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date('1984-10-11'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date('1984-10-11'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date('1984-10-11'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('14:30'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('14:30'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(time('14:30'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(time('14:30'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(time('14:30'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(time('14:30'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(time('14:30'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('2015-07-21T21:40:32.142'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('2015-07-21T21:40:32.142'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('2015-07-21T21:40:32.142'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('2015-07-21T21:40:32.142'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime('2014-07-21T21:40:36.143+0200'), date('2015-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime('2014-07-21T21:40:36.143+0200'), localdatetime('2016-07-21T21:45:22.142')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime('2014-07-21T21:40:36.143+0200'), datetime('2015-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime('2014-07-21T21:40:36.143+0200'), localtime('16:30')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime('2014-07-21T21:40:36.143+0200'), time('16:30+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('2014-07-21T21:40:36.143'), localdatetime('2014-07-21T21:40:36.142')) AS d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('2018-03-11'), date('2016-06-24')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(date('2018-07-21'), datetime('2016-07-21T21:40:32.142+0100')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(localdatetime('2018-07-21T21:40:32.142'), date('2016-07-21')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2018-07-21T21:40:36.143+0200'), localdatetime('2016-07-21T21:40:36.143')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(datetime('2018-07-21T21:40:36.143+0500'), datetime('1984-07-21T22:40:36.143+0200')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localdatetime({year: 2017, month: 10, day: 29, hour: 4})) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localtime({hour: 4})) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime({year: 2017, month: 10, day: 29, hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime({hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date({year: 2017, month: 10, day: 29}), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), date({year: 2017, month: 10, day: 30})) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(date('-999999999-01-01'), date('+999999999-12-31')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime('-999999999-01-01'), localdatetime('+999999999-12-31T23:59:59')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:54.7'), localtime('12:34:54.3')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:54.3'), localtime('12:34:54.7')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:54.7'), localtime('12:34:55.3')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:54.7'), localtime('12:44:55.3')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:44:54.7'), localtime('12:34:55.3')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:56'), localtime('12:34:55.7')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:56'), localtime('12:44:55.7')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:44:56'), localtime('12:34:55.7')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:56.3'), localtime('12:34:54.7')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime('12:34:54.7'), localtime('12:34:56.3')) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localtime(), localtime()) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(time(), time()) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(date(), date()) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(localdatetime(), localdatetime()) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(datetime(), datetime()) AS duration

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.between(null, null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inMonths(null, null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inDays(null, null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal10.feature
RETURN duration.inSeconds(null, null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015-07-21') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('20150721') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015-07') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('201507') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015-W30-2') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015W302') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015-W30') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015W30') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015-202') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015202') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN date('2015') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('21:40:32.142') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('214032.142') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('21:40:32') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('214032') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('21:40') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('2140') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localtime('21') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('21:40:32.142+0100') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('214032.142Z') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('21:40:32+01:00') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('214032-0100') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('21:40-01:30') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('2140-00:00') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('2140-02') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN time('22+18:00') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('2015-07-21T21:40:32.142') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('2015-W30-2T214032.142') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('2015-202T21:40:32') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('2015T214032') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('20150721T21:40') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('2015-W30T2140') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN localdatetime('2015202T21') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-07-21T21:40:32.142+0100') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-W30-2T214032.142Z') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-202T21:40:32+01:00') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015T214032-0100') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('20150721T21:40-01:30') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-W30T2140-00:00') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-W30T2140-02') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015202T21+18:00') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-07-21T21:40:32.142+02:00[Europe/Stockholm]') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-07-21T21:40:32.142+0845[Australia/Eucla]') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-07-21T21:40:32.142-04[America/New_York]') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('2015-07-21T21:40:32.142[Europe/London]') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN datetime('1818-07-21T21:40:32.142[Europe/Stockholm]') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('P14DT16H12M') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('P5M1.5D') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('P0.75M') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('PT0.75M') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('P2.5W') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('P12Y5M14DT16H12M70S') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal2.feature
RETURN duration('P2012-02-02T14:37:21.545') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date({date: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date({date: other, year: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date({date: other, day: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date({date: other, week: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date({date: other, ordinalDay: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 11, day: 11}) AS other
RETURN date({date: other, quarter: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date({date: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date({date: other, year: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date({date: other, day: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date({date: other, week: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date({date: other, ordinalDay: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN date({date: other, quarter: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date({date: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date({date: other, year: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date({date: other, day: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date({date: other, week: 1}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date({date: other, ordinalDay: 28}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN date({date: other, quarter: 3}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN localtime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN localtime({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN localtime({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN localtime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN localtime({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN localtime({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localtime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localtime({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localtime({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localtime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localtime({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localtime({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN time(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN time({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN time({time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN time({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN time({time: other, second: 42, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN time(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN time({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN time({time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN time({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN time({time: other, second: 42, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN time(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN time({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN time({time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN time({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN time({time: other, second: 42, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN time(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN time({time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN time({time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN time({time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN time({time: other, second: 42, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS other
RETURN localdatetime({date: other, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS other
RETURN localdatetime({date: other, day: 28, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime({date: other, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime({date: other, day: 28, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime({date: other, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime({date: other, day: 28, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherTime
RETURN localdatetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime({datetime: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN localdatetime({datetime: other, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime({datetime: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN localdatetime({datetime: other, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS other
RETURN datetime({date: other, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS other
RETURN datetime({date: other, hour: 10, minute: 10, second: 10, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS other
RETURN datetime({date: other, day: 28, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS other
RETURN datetime({date: other, day: 28, hour: 10, minute: 10, second: 10, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({date: other, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({date: other, hour: 10, minute: 10, second: 10, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({date: other, day: 28, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({date: other, day: 28, hour: 10, minute: 10, second: 10, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN datetime({date: other, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN datetime({date: other, hour: 10, minute: 10, second: 10, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN datetime({date: other, day: 28, hour: 10, minute: 10, second: 10}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS other
RETURN datetime({date: other, day: 28, hour: 10, minute: 10, second: 10, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({year: 1984, month: 10, day: 11, time: other, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH date({year: 1984, month: 10, day: 11}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'}) AS otherDate, datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime
RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({datetime: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({datetime: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({datetime: other, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645}) AS other
RETURN datetime({datetime: other, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime(other) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({datetime: other}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({datetime: other, timezone: '+05:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({datetime: other, day: 28, second: 42}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal3.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS other
RETURN datetime({datetime: other, day: 28, second: 42, timezone: 'Pacific/Honolulu'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({created: date({year: 1984, month: 10, day: 11})})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [date({year: 1984, month: 10, day: 12})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [date({year: 1984, month: 10, day: 13}), date({year: 1984, month: 10, day: 14}), date({year: 1984, month: 10, day: 15})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({created: localtime({hour: 12})})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [localtime({hour: 13})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [localtime({hour: 14}), localtime({hour: 15}), localtime({hour: 16})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({created: time({hour: 12})})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [time({hour: 13})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [time({hour: 14}), time({hour: 15}), time({hour: 16})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({created: localdatetime({year: 1912})})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [localdatetime({year: 1913})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [localdatetime({year: 1914}), localdatetime({year: 1915}), localdatetime({year: 1916})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({created: datetime({year: 1912})})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [datetime({year: 1913})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [datetime({year: 1914}), datetime({year: 1915}), datetime({year: 1916})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({created: duration({seconds: 12})})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [duration({seconds: 13})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
CREATE ({dates: [duration({seconds: 14}), duration({seconds: 15}), duration({seconds: 16})]})

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN date(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN date.transaction(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN date.statement(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN date.realtime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localtime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localtime.transaction(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localtime.statement(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localtime.realtime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN time(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN time.transaction(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN time.statement(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN time.realtime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localdatetime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localdatetime.transaction(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localdatetime.statement(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN localdatetime.realtime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN datetime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN datetime.transaction(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN datetime.statement(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN datetime.realtime(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal4.feature
RETURN duration(null) AS t

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.year, d.weekYear, d.week, d.weekDay

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond, d.timezone, d.offset, d.offsetMinutes, d.offsetSeconds

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter,
       d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter,
       d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond,
       d.timezone, d.offset, d.offsetMinutes, d.offsetSeconds, d.epochSeconds, d.epochMillis

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal5.feature
MATCH (v:Val)
WITH v.date AS d
RETURN d.years, d.quarters, d.months, d.weeks, d.days,
       d.hours, d.minutes, d.seconds, d.milliseconds, d.microseconds, d.nanoseconds,
       d.quartersOfYear, d.monthsOfQuarter, d.monthsOfYear, d.daysOfWeek, d.minutesOfHour, d.secondsOfMinute, d.millisecondsOfSecond, d.microsecondsOfSecond, d.nanosecondsOfSecond

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH date({year: 1984, month: 10, day: 11}) AS d
RETURN toString(d) AS ts, date(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN toString(d) AS ts, localtime(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS d
RETURN toString(d) AS ts, time(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN toString(d) AS ts, localdatetime(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}) AS d
RETURN toString(d) AS ts, datetime(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({years: 12, months: 5, days: -14, hours: 16}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({minutes: 12, seconds: -60}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({seconds: 2, milliseconds: -1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({seconds: -2, milliseconds: 1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({seconds: -2, milliseconds: -1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({days: 1, milliseconds: 1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({days: 1, milliseconds: -1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({seconds: 60, milliseconds: -1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({seconds: -60, milliseconds: 1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH duration({seconds: -60, milliseconds: -1}) AS d
RETURN toString(d) AS ts, duration(toString(d)) = d AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal6.feature
WITH datetime({year: 2017, month: 8, day: 8, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) AS d
RETURN toString(d) AS ts

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH date({year: 1980, month: 12, day: 24}) AS x, date({year: 1984, month: 10, day: 11}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH date({year: 1984, month: 10, day: 11}) AS x, date({year: 1984, month: 10, day: 11}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH localtime({hour: 10, minute: 35}) AS x, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS x, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH time({hour: 10, minute: 0, timezone: '+01:00'}) AS x, time({hour: 9, minute: 35, second: 14, nanosecond: 645876123, timezone: '+00:00'}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH time({hour: 9, minute: 35, second: 14, nanosecond: 645876123, timezone: '+00:00'}) AS x, time({hour: 9, minute: 35, second: 14, nanosecond: 645876123, timezone: '+00:00'}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH localdatetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14}) AS x, localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS x, localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH datetime({year: 1980, month: 12, day: 11, hour: 12, minute: 31, second: 14, timezone: '+00:00'}) AS x, datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+05:00'}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+05:00'}) AS x, datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+05:00'}) AS d
RETURN x > d, x < d, x >= d, x <= d, x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, date({year: 1984, month: 10, day: 11}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, time({hour: 9, minute: 35, second: 14, nanosecond: 645876123, timezone: '+00:00'}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+05:00'}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, duration({years: 12, months: 5, days: 14, hours: 16, minutes: 13, seconds: 10}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal7.feature
WITH duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) AS x, duration({years: 12, months: 5, days: 13, hours: 40, minutes: 13, seconds: 10}) AS d
RETURN x = d

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH date({year: 1984, month: 10, day: 11}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH date({year: 1984, month: 10, day: 11}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH date({year: 1984, month: 10, day: 11}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 1}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 1}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 1}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH time({hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH time({hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH time({hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 1}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 1}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 1}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
WITH datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'}) AS x
MATCH (d:Duration)
RETURN x + d.dur AS sum, x - d.dur AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (dur:Duration1), (dur2: Duration2)
RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (d:Duration)
RETURN d.date * 1 AS prod, d.date / 1 AS div

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (d:Duration)
RETURN d.date * 2 AS prod, d.date / 2 AS div

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal8.feature
MATCH (d:Duration)
RETURN d.date * 0.5 AS prod, d.date / 0.5 AS div

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('millennium', date({year: 2017, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('millennium', date({year: 2017, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('century', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('century', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('century', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('century', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('century', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('century', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('decade', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('decade', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('year', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('year', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('quarter', date({year: 1984, month: 11, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('quarter', date({year: 1984, month: 11, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('month', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('month', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('week', date({year: 1984, month: 10, day: 11}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('week', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('day', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN date.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', date({year: 2017, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', date({year: 2017, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', date({year: 2017, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', date({year: 2017, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('century', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', date({year: 1984, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', date({year: 1984, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', date({year: 1984, month: 11, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', date({year: 1984, month: 11, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', date({year: 1984, month: 11, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', date({year: 1984, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', date({year: 1984, month: 10, day: 11}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', date({year: 1984, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', date({year: 1984, month: 10, day: 11}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', date({year: 1984, month: 10, day: 11}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: 'Europe/Stockholm'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN datetime.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millennium', date({year: 2017, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millennium', date({year: 2017, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millennium', datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millennium', localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('century', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('century', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('century', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('century', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('century', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('century', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('decade', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('decade', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('decade', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('decade', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('year', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('year', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('year', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('year', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('weekYear', date({year: 1984, month: 2, day: 1}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('weekYear', datetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 5}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('weekYear', localdatetime({year: 1984, month: 1, day: 1, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('quarter', date({year: 1984, month: 11, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('quarter', date({year: 1984, month: 11, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('quarter', datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('quarter', localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('month', date({year: 1984, month: 10, day: 11}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('month', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('month', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {day: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('month', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('week', date({year: 1984, month: 10, day: 11}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('week', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('week', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {dayOfWeek: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('week', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('day', date({year: 1984, month: 10, day: 11}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('day', date({year: 1984, month: 10, day: 11}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localdatetime.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('hour', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('minute', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('second', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('millisecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN localtime.truncate('microsecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('day', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('day', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {timezone: '+01:00'}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('hour', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('minute', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '-01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('second', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('millisecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {nanosecond: 2}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/temporal/Temporal9.feature
RETURN time.truncate('microsecond', time({hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}), {}) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
UNWIND [true, false] AS b
RETURN toBoolean(b) AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
RETURN toBoolean('true') AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
UNWIND ['true', 'false'] AS s
RETURN toBoolean(s) AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
UNWIND [null, '', ' tru ', 'f alse'] AS things
RETURN toBoolean(things) AS b

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [true, []] | toBoolean(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [true, {}] | toBoolean(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [true, 1.0] | toBoolean(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [true, n] | toBoolean(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [true, r] | toBoolean(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion1.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [true, p] | toBoolean(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
WITH 82.9 AS weight
RETURN toInteger(weight)

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
WITH 'foo' AS foo_string, '' AS empty_string
RETURN toInteger(foo_string) AS foo, toInteger(empty_string) AS empty

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
WITH [2, 2.9] AS numbers
RETURN [n IN numbers | toInteger(n)] AS int_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
WITH [2, 2.9, '1.7'] AS things
RETURN [n IN things | toInteger(n)] AS int_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
WITH ['2', '2.9', 'foo'] AS numbers
RETURN [n IN numbers | toInteger(n)] AS int_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
RETURN toInteger(1 - $param) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
MATCH (p:Person { name: '42' })
WITH *
MATCH (n)
RETURN toInteger(n.name) AS name

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, []] | toInteger(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, {}] | toInteger(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, n] | toInteger(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, r] | toInteger(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion2.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, p] | toInteger(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
WITH [3.4, 3] AS numbers
RETURN [n IN numbers | toFloat(n)] AS float_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
WITH 'foo' AS foo_string, '' AS empty_string
RETURN toFloat(foo_string) AS foo, toFloat(empty_string) AS empty

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
WITH [3.4, 3, '5'] AS numbers
RETURN [n IN numbers | toFloat(n)] AS float_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
WITH ['1', '2', 'foo'] AS numbers
RETURN [n IN numbers | toFloat(n)] AS float_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH (m:Movie { rating: 4 })
WITH *
MATCH (n)
RETURN toFloat(n.rating) AS float

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1.0, true] | toFloat(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1.0, []] | toFloat(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1.0, {}] | toFloat(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1.0, n] | toFloat(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1.0, r] | toFloat(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion3.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1.0, p] | toFloat(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
RETURN toString(42) AS bool

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
RETURN toString(true) AS bool

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
RETURN toString(1 < 0) AS bool

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH (m:Movie)
RETURN toString(m.watched)

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
RETURN [x IN [1, 2.3, true, 'apa'] | toString(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
WITH [1, 2, 3] AS numbers
RETURN [n IN numbers | toString(n)] AS string_numbers

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH (m:Movie { rating: 4 })
WITH *
MATCH (n)
RETURN toString(n.rating)

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
UNWIND ['male', 'female', null] AS gen
RETURN coalesce(toString(gen), 'x') AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
UNWIND ['male', 'female', null] AS gen
RETURN toString(coalesce(gen, 'x')) AS result

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, '', []] | toString(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, '', {}] | toString(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, '', n] | toString(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, '', r] | toString(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/expressions/typeConversion/TypeConversion4.feature
MATCH p = (n)-[r:T]->()
RETURN [x IN [1, '', p] | toString(x) ] AS list

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH ()--()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH (n)--(n)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH ()--()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH ()-->()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH (n)-->(n)
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH (n)-[r]-(n)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH (n)-[r]-(n)
RETURN count(DISTINCT r)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH ()-->()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH (n)-[r]->(n)
RETURN count(r)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH (:A)-->()--()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature
MATCH ()-[]-()-[]-()
RETURN count(*)

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r:FOLLOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-->(b)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS|FOLLOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b:X)-->(c:X)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b:X)-->(c:Y)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c:X)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b:X)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r:FOLLOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-->(b)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS|FOLLOWS]->(b)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b:X)-->(c:X)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b:X)-->(c:Y)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b)-->(c:X)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

// ../../cypher-tck/tck-M23/tck/features/useCases/triadicSelection/TriadicSelection1.feature
MATCH (a:A)-[:KNOWS]->(b:X)-->(c)
OPTIONAL MATCH (a)-[r:KNOWS]->(c)
WITH c WHERE r IS NOT NULL
RETURN c.name

