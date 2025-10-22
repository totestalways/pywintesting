-- Per table, between timestamps
VAR t1 TIMESTAMP; VAR t2 TIMESTAMP;
EXEC :t1 := TO_TIMESTAMP('2025-10-22 09:00','YYYY-MM-DD HH24:MI');
EXEC :t2 := TO_TIMESTAMP('2025-10-22 17:00','YYYY-MM-DD HH24:MI');

SELECT versions_operation AS operation, COUNT(*) AS ops
FROM   my_schema.my_table
       VERSIONS BETWEEN TIMESTAMP :t1 AND :t2
GROUP  BY versions_operation;  -- I/U/D
