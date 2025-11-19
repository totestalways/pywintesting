SELECT 'SELECT ''' || table_name || ''' AS table_name, COUNT(*) AS cnt
        FROM ' || table_name || ' WHERE x = :val HAVING COUNT(*) > 0'
FROM   user_tab_columns
WHERE  column_name = 'X';
