CREATE OR REPLACE FUNCTION estimate_time_from_scn(p_scn IN NUMBER)
  RETURN TIMESTAMP
IS
  v_prev_time  TIMESTAMP;
  v_time_dp    TIMESTAMP;
  v_prev_scn   NUMBER;
  v_scn        NUMBER;
  v_result     TIMESTAMP;
BEGIN
  SELECT prev_time, time_dp, prev_scn, scn
  INTO   v_prev_time, v_time_dp, v_prev_scn, v_scn
  FROM (
    SELECT time_dp,
           scn,
           LAG(time_dp) OVER (ORDER BY scn) AS prev_time,
           LAG(scn)     OVER (ORDER BY scn) AS prev_scn
    FROM   smon_scn_time
  )
  WHERE  p_scn BETWEEN prev_scn AND scn
  FETCH FIRST 1 ROW ONLY;

  IF v_prev_scn IS NULL OR v_scn = v_prev_scn THEN
    RETURN NULL;
  END IF;

  v_result := v_prev_time
              + NUMTODSINTERVAL(
                  (p_scn - v_prev_scn)
                  * ( (CAST(v_time_dp AS DATE) - CAST(v_prev_time AS DATE)) * 86400 ),
                  'SECOND'
                );
  RETURN v_result;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN NULL; -- outside SMON_SCN_TIME window
END;
/
