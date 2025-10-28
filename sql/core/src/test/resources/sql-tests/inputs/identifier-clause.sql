-------------------------------------------------------------------------------------------------
-- Ths testfile tests the ability to templatize identifiers such as table and column names in SQL
-- Common patterns are to to use variable substitution or parameter markers (test in another file)
--------------------------------------------------------------------------------------------------

-- Sanity test variable substitution
SET hivevar:colname = 'c';
SELECT IDENTIFIER(${colname} || '_1') FROM VALUES(1) AS T(c_1);

-- Column references
SELECT IDENTIFIER('c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('t.c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('`t`.c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('`c 1`') FROM VALUES(1) AS T(`c 1`);
SELECT IDENTIFIER('``') FROM VALUES(1) AS T(``);
SELECT IDENTIFIER('c' || '1') FROM VALUES(1) AS T(c1);

-- Table references
CREATE SCHEMA IF NOT EXISTS s;
CREATE TABLE s.tab(c1 INT) USING CSV;
USE SCHEMA s;

INSERT INTO IDENTIFIER('ta' || 'b') VALUES(1);
DELETE FROM IDENTIFIER('ta' || 'b') WHERE 1=0;
UPDATE IDENTIFIER('ta' || 'b') SET c1 = 2;
MERGE INTO IDENTIFIER('ta' || 'b') AS t USING IDENTIFIER('ta' || 'b') AS s ON s.c1 = t.c1
  WHEN MATCHED THEN UPDATE SET c1 = 3;
SELECT * FROM IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s.tab');
SELECT * FROM IDENTIFIER('`s`.`tab`');
SELECT * FROM IDENTIFIER('t' || 'a' || 'b');

USE SCHEMA default;
DROP TABLE s.tab;
DROP SCHEMA s;

-- Function reference
SELECT IDENTIFIER('COAL' || 'ESCE')(NULL, 1);
SELECT IDENTIFIER('abs')(c1) FROM VALUES(-1) AS T(c1);
SELECT * FROM IDENTIFIER('ra' || 'nge')(0, 1);

-- Table DDL
CREATE TABLE IDENTIFIER('tab')(c1 INT) USING CSV;
DROP TABLE IF EXISTS IDENTIFIER('ta' || 'b');

CREATE SCHEMA identifier_clauses;
USE identifier_clauses;
CREATE TABLE IDENTIFIER('ta' || 'b')(c1 INT) USING CSV;
DROP TABLE IF EXISTS IDENTIFIER('identifier_clauses.' || 'tab');
CREATE TABLE IDENTIFIER('identifier_clauses.' || 'tab')(c1 INT) USING CSV;
REPLACE TABLE IDENTIFIER('identifier_clauses.' || 'tab')(c1 INT) USING CSV;
CACHE TABLE IDENTIFIER('ta' || 'b');
UNCACHE TABLE IDENTIFIER('ta' || 'b');
DROP TABLE IF EXISTS IDENTIFIER('ta' || 'b');
USE default;
DROP SCHEMA identifier_clauses;

CREATE TABLE tab(c1 INT) USING CSV;
INSERT INTO tab VALUES (1);
SELECT c1 FROM tab;
DESCRIBE IDENTIFIER('ta' || 'b');
ANALYZE TABLE IDENTIFIER('ta' || 'b') COMPUTE STATISTICS;
ALTER TABLE IDENTIFIER('ta' || 'b') ADD COLUMN c2 INT;
SHOW TBLPROPERTIES IDENTIFIER('ta' || 'b');
SHOW COLUMNS FROM IDENTIFIER('ta' || 'b');
COMMENT ON TABLE IDENTIFIER('ta' || 'b') IS 'hello';
REFRESH TABLE IDENTIFIER('ta' || 'b');
REPAIR TABLE IDENTIFIER('ta' || 'b');
TRUNCATE TABLE IDENTIFIER('ta' || 'b');
DROP TABLE IF EXISTS tab;

-- View
CREATE OR REPLACE VIEW IDENTIFIER('v')(c1) AS VALUES(1);
SELECT * FROM v;
ALTER VIEW IDENTIFIER('v') AS VALUES(2);
DROP VIEW IDENTIFIER('v');
CREATE TEMPORARY VIEW IDENTIFIER('v')(c1) AS VALUES(1);
DROP VIEW IDENTIFIER('v');

-- Schema
CREATE SCHEMA IDENTIFIER('id' || 'ent');
ALTER SCHEMA IDENTIFIER('id' || 'ent') SET PROPERTIES (somekey = 'somevalue');
ALTER SCHEMA IDENTIFIER('id' || 'ent') SET LOCATION 'someloc';
COMMENT ON SCHEMA IDENTIFIER('id' || 'ent') IS 'some comment';
DESCRIBE SCHEMA IDENTIFIER('id' || 'ent');
SHOW TABLES IN IDENTIFIER('id' || 'ent');
SHOW TABLE EXTENDED IN IDENTIFIER('id' || 'ent') LIKE 'hello';
USE IDENTIFIER('id' || 'ent');
SHOW CURRENT SCHEMA;
USE SCHEMA IDENTIFIER('id' || 'ent');
USE SCHEMA default;
DROP SCHEMA IDENTIFIER('id' || 'ent');

-- Function
CREATE SCHEMA ident;
CREATE FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg') AS 'test.org.apache.spark.sql.MyDoubleAvg';
DESCRIBE FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg');
REFRESH FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg');
DROP FUNCTION IDENTIFIER('ident.' || 'myDoubleAvg');
DROP SCHEMA ident;
CREATE TEMPORARY FUNCTION IDENTIFIER('my' || 'DoubleAvg') AS 'test.org.apache.spark.sql.MyDoubleAvg';
DROP TEMPORARY FUNCTION IDENTIFIER('my' || 'DoubleAvg');

-- IDENTIFIER + variable
DECLARE var = 'sometable';
CREATE TABLE IDENTIFIER(var)(c1 INT) USING CSV;

SET VAR var = 'c1';
SELECT IDENTIFIER(var) FROM VALUES(1) AS T(c1);

SET VAR var = 'some';
DROP TABLE IDENTIFIER(var || 'table');

-- Error conditions
SELECT IDENTIFIER('c 1') FROM VALUES(1) AS T(`c 1`);
SELECT IDENTIFIER('') FROM VALUES(1) AS T(``);
VALUES(IDENTIFIER(CAST(NULL AS STRING)));
VALUES(IDENTIFIER(1));
VALUES(IDENTIFIER(SUBSTR('HELLO', 1, RAND() + 1)));
SELECT `IDENTIFIER`('abs')(c1) FROM VALUES(-1) AS T(c1);

CREATE TABLE IDENTIFIER(1)(c1 INT) USING csv;
CREATE TABLE IDENTIFIER('a.b.c')(c1 INT) USING csv;
CREATE VIEW IDENTIFIER('a.b.c')(c1) AS VALUES(1);
DROP TABLE IDENTIFIER('a.b.c');
DROP VIEW IDENTIFIER('a.b.c');
COMMENT ON TABLE IDENTIFIER('a.b.c.d') IS 'hello';
VALUES(IDENTIFIER(1)());
VALUES(IDENTIFIER('a.b.c.d')());

CREATE TEMPORARY FUNCTION IDENTIFIER('default.my' || 'DoubleAvg') AS 'test.org.apache.spark.sql.MyDoubleAvg';
DROP TEMPORARY FUNCTION IDENTIFIER('default.my' || 'DoubleAvg');
CREATE TEMPORARY VIEW IDENTIFIER('default.v')(c1) AS VALUES(1);

-- SPARK-48273: Aggregation operation in statements using identifier clause for table name
create temporary view identifier('v1') as (select my_col from (values (1), (2), (1) as (my_col)) group by 1);
cache table identifier('t1') as (select my_col from (values (1), (2), (1) as (my_col)) group by 1);
create table identifier('t2') using csv as (select my_col from (values (1), (2), (1) as (my_col)) group by 1);
insert into identifier('t2') select my_col from (values (3) as (my_col)) group by 1;
drop view v1;
drop table t1;
drop table t2;

-- SPARK-46625: CTE reference with identifier clause and session variables
DECLARE agg = 'max';
DECLARE col = 'c1';
DECLARE tab = 'T';

WITH S(c1, c2) AS (VALUES(1, 2), (2, 3)),
     T(c1, c2) AS (VALUES ('a', 'b'), ('c', 'd'))
SELECT IDENTIFIER(agg)(IDENTIFIER(col)) FROM IDENTIFIER(tab);

WITH S(c1, c2) AS (VALUES(1, 2), (2, 3)),
     T(c1, c2) AS (VALUES ('a', 'b'), ('c', 'd'))
SELECT IDENTIFIER('max')(IDENTIFIER('c1')) FROM IDENTIFIER('T');

WITH ABC(c1, c2) AS (VALUES(1, 2), (2, 3))
SELECT IDENTIFIER('max')(IDENTIFIER('c1')) FROM IDENTIFIER('A' || 'BC');

-- Identifier-lite: Tests for string literal-only IDENTIFIER() usage
-- These tests verify that IDENTIFIER('literal') works in all identifier positions
-- Note: The difference from tests above is these use ONLY string literals (no expressions/variables)

-- Identifier-lite in column definitions
CREATE TABLE IDENTIFIER('id_lite_col_test')(IDENTIFIER('col1') INT, IDENTIFIER('col2') STRING) USING CSV;
INSERT INTO IDENTIFIER('id_lite_col_test') VALUES (1, 'test');
SELECT IDENTIFIER('col1'), IDENTIFIER('col2') FROM IDENTIFIER('id_lite_col_test');
DROP TABLE IDENTIFIER('id_lite_col_test');

-- Identifier-lite in ALTER TABLE operations
CREATE TABLE IDENTIFIER('id_lite_alter')(c1 INT) USING CSV;
ALTER TABLE IDENTIFIER('id_lite_alter') RENAME COLUMN IDENTIFIER('c1') TO IDENTIFIER('col1');
ALTER TABLE IDENTIFIER('id_lite_alter') ADD COLUMN IDENTIFIER('c2') INT;
ALTER TABLE IDENTIFIER('id_lite_alter') DROP COLUMN IDENTIFIER('c2');
ALTER TABLE IDENTIFIER('id_lite_alter') RENAME TO IDENTIFIER('id_lite_renamed');
DROP TABLE IDENTIFIER('id_lite_renamed');

-- Identifier-lite with multiple qualified parts
CREATE SCHEMA identifier_lite_schema;
CREATE TABLE IDENTIFIER('identifier_lite_schema.qualified_test')(c1 INT) USING CSV;
INSERT INTO IDENTIFIER('identifier_lite_schema.qualified_test') VALUES(42);
SELECT * FROM IDENTIFIER('identifier_lite_schema.qualified_test');
DROP TABLE IDENTIFIER('identifier_lite_schema.qualified_test');
DROP SCHEMA identifier_lite_schema;

-- Identifier-lite with qualified identifiers in different positions
CREATE SCHEMA cat1;
CREATE TABLE cat1.tab1(c1 INT) USING CSV;

-- IDENTIFIER('schema').table syntax
INSERT INTO IDENTIFIER('cat1').tab1 VALUES(1);
SELECT * FROM IDENTIFIER('cat1').tab1;

-- IDENTIFIER('schema.table') syntax
SELECT * FROM IDENTIFIER('cat1.tab1');

-- Mixed: IDENTIFIER('schema').IDENTIFIER('table')
SELECT * FROM IDENTIFIER('cat1').IDENTIFIER('tab1');

DROP TABLE cat1.tab1;
DROP SCHEMA cat1;

-- Identifier-lite with backticks in qualified names
CREATE SCHEMA `schema 1`;
CREATE TABLE `schema 1`.`table 1`(c1 INT) USING CSV;

-- Use identifier-lite with backticked qualified name
INSERT INTO IDENTIFIER('`schema 1`.`table 1`') VALUES(100);
SELECT * FROM IDENTIFIER('`schema 1`.`table 1`');

-- Mixed: IDENTIFIER for schema part, regular for table
SELECT * FROM IDENTIFIER('`schema 1`').`table 1`;

DROP TABLE `schema 1`.`table 1`;
DROP SCHEMA `schema 1`;

-- Not supported
SELECT row_number() OVER IDENTIFIER('x.win') FROM VALUES(1) AS T(c1) WINDOW win AS (ORDER BY c1);
SELECT T1.c1 FROM VALUES(1) AS T1(c1) JOIN VALUES(1) AS T2(c1) USING (IDENTIFIER('c1'));
SELECT IDENTIFIER('t').c1 FROM VALUES(1) AS T(c1);
SELECT map('a', 1).IDENTIFIER('a') FROM VALUES(1) AS T(c1);
SELECT named_struct('a', 1).IDENTIFIER('a') FROM VALUES(1) AS T(c1);
SELECT * FROM s.IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s').IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s').tab;
SELECT row_number() OVER IDENTIFIER('win') FROM VALUES(1) AS T(c1) WINDOW win AS (ORDER BY c1);
SELECT row_number() OVER win FROM VALUES(1) AS T(c1) WINDOW IDENTIFIER('win') AS (ORDER BY c1);
WITH identifier('v')(identifier('c1')) AS (VALUES(1)) (SELECT c1 FROM v);
INSERT INTO tab(IDENTIFIER('c1')) VALUES(1);
CREATE OR REPLACE VIEW v(IDENTIFIER('c1')) AS VALUES(1);
CREATE TABLE tab(IDENTIFIER('c1') INT) USING CSV;

-- Identifier-lite: Column definitions should work with string literals
-- (This is a positive test showing identifier-lite works in column definitions)
CREATE TABLE IDENTIFIER('id_lite_coldef_ok')(IDENTIFIER('c1') INT) USING CSV;
DROP TABLE IDENTIFIER('id_lite_coldef_ok');

-- Identifier-lite: Error when qualified identifier used in single identifier context
-- This should error because 'col1.col2' is qualified but column name must be single
CREATE TABLE test_qualified_col_error(IDENTIFIER('col1.col2') INT) USING CSV;

-- This should error because 'schema.table' is qualified but used as column name
CREATE TABLE test_qualified_col_error2(id INT, IDENTIFIER('schema.table') STRING) USING CSV;

-- Correct way: use backticks to create a single identifier with a dot
CREATE TABLE test_col_with_dot(IDENTIFIER('`col.with.dot`') INT) USING CSV;
DROP TABLE test_col_with_dot;

-- Identifier-lite in column aliases (AS clause)
SELECT 1 AS IDENTIFIER('col1');
SELECT 'hello' AS IDENTIFIER('my_column');

-- Identifier-lite in table value constructor with table and column aliases
SELECT * FROM VALUES (1, 2) AS IDENTIFIER('my_table')(IDENTIFIER('c1'), IDENTIFIER('c2'));
SELECT * FROM VALUES (10, 20) AS IDENTIFIER('t')(IDENTIFIER('col_a'), IDENTIFIER('col_b'));

-- Identifier-lite: table alias with qualified name should error (table alias must be single)
SELECT * FROM VALUES (1, 2) AS IDENTIFIER('schema.table')(c1, c2);

-- Identifier-lite: column alias with qualified name should error (column alias must be single)
SELECT 1 AS IDENTIFIER('col1.col2');
