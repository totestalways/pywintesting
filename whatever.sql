SELECT versions_operation AS operation, COUNT(*) AS ops
FROM   my_schema.my_table
       VERSIONS BETWEEN SCN
         TIMESTAMP_TO_SCN(TO_TIMESTAMP('2025-10-22 09:00','YYYY-MM-DD HH24:MI'))
     AND TIMESTAMP_TO_SCN(TO_TIMESTAMP('2025-10-22 17:00','YYYY-MM-DD HH24:MI'))
GROUP  BY versions_operation;
