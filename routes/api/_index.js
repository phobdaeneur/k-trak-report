const express = require("express");
const router = express.Router();
const { pool_zb } = require("../../config/db");
const ApiError = require("../../error/ApiError");
const isAuth = require("../../middlewares/isAuth");

/* GET api/fleets */
router.get("/fleets/:loginName", isAuth, async (req, res, next) => {
  /**
   * Extract path parameter
   */
  const { loginName } = req.params;

  try {
    const pool_zbq = await pool_zb();
    const result = await pool_zbq.query(`
      SELECT c.fleet_id
      ,c.fleet_desc
      FROM [ZebraDB].[dbo].[staff] a
      left join [ZebraDB].[dbo].[profile_fleet] b on a.profile_id = b.profile_id
      left join [ZebraDB].[dbo].[fleet] c on b.fleet_id = c.fleet_id
      where login_name = '${loginName}'
    `);

    return res.status(200).json(result.recordset);
  } catch (err) {
    next(err);
  }
});

/* GET api/fleet/vehicles */
router.get("/fleet/vehicles/:fleetId", isAuth, async (req, res, next) => {
  const { fleetId } = req.params;

  if (!fleetId) {
    return next(ApiError.badRequest("Invalid credentials!"));
  } else {
    try {
      const pool_zbq = await pool_zb();

      const result = await pool_zbq.query(`
      SELECT 
       a.fleet_id
      ,a.veh_id
      ,d.registration
      ,b.lat
      ,b.lon
      ,c.local_timestamp
      ,c.speed
      ,c.name
      ,c.namt
      ,b.distance
      ,e.evt_id
      ,ISNULL(ZebraDB.dbo.IsEngineOnTheLastByTime_Log(a.veh_id ,c.local_timestamp),0)
      ,case 
      when isnull(e.evt_id,12) = 2 
      and isnull(c.local_timestamp,GETDATE()-30) between (dateadd(hour,-12,getdate())) and (dateadd(hour,1,getdate())) then 'SPEEDING'
      when isnull(e.evt_id,12) = 3 
      and isnull(c.local_timestamp,GETDATE()-30) between (dateadd(hour,-12,getdate())) and (dateadd(hour,1,getdate())) then 'IDLE'
      when isnull(c.local_timestamp,GETDATE()-30) between (dateadd(hour,-24,getdate())) and (dateadd(hour,-12,getdate())) then 'NO SIGNAL 12Hr.'
      when isnull(c.local_timestamp,GETDATE()-30) < (dateadd(hour,-24,getdate())) then 'NO SIGNAL 24Hr.'
      when isnull(e.evt_id,12) not in (2,3) 
      and isnull(c.local_timestamp,GETDATE()-30) > (dateadd(hour,-12,getdate())) 
      and ISNULL(ZebraDB.dbo.IsEngineOnTheLastByTime_Log(a.veh_id ,c.local_timestamp),0) = 0 then 'ENGINE OFF'
      else 'NORMAL'
      end as Status
      ,case when f.tag_msg is null then  '?' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New(a.veh_id,0,c.local_timestamp) , '999.99','-') AS varchar) end AS Temp1
      ,case when f.tag_msg is null then  '?' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New(a.veh_id,1,c.local_timestamp) , '999.99','-') AS varchar) end AS Temp2
      ,case when f.tag_msg is null then  '?' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New(a.veh_id,2,c.local_timestamp) , '999.99','-') AS varchar) end AS Temp3
      ,case when f.tag_msg is null then  '?' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New(a.veh_id,3,c.local_timestamp) , '999.99','-') AS varchar) end AS Temp4
        FROM  [ZebraDB].[dbo].[fleet_vehicle] a
        left join [ZebraDB_Log].[dbo].[veh_current_location] b on a.veh_id =b.veh_id
        left join [ZebraDB_Log].[dbo].[log_msg] c on b.ref_idx = c.idx
        left join [ZebraDB].[dbo].[vehicle] d on a.veh_id = d.veh_id
        Left join [ZebraDB_Log].[dbo].[veh_current_event] e on c.idx = e.ref_idx
        left join [ZebraDB_Log].[dbo].[log_msg_tag] f on c.idx = f.ref_idx
        where a.fleet_id = ${parseInt(fleetId)}
    `);

      return res.status(200).json(result.recordset);
    } catch (err) {
      next(err);
    }
  }
});

/* GET api/fleets/vehicleReport */
router.get(
  "/fleet/vehicleReport/:vehicleId/:dateStart/:dateEnd",
  isAuth,
  async (req, res, next) => {
    const { vehicleId, dateStart, dateEnd } = req.params;
    console.log(vehicleId);
    console.log(dateStart);
    console.log(dateEnd);

    if (!vehicleId || !dateStart || !dateEnd) {
      return next(ApiError.badRequest("Invalid credentials!"));
    } else {
      try {
        const pool_zbq = await pool_zb();

        const result = await pool_zbq.query(`
        DECLARE @FirstDistance FLOAT
 DECLARE @vehicleId INT 
 DECLARE @dateStart DATETIME 
 DECLARE @dateEnd DATETIME 

SET @vehicleId = ${vehicleId}
SET @dateStart = '${dateStart}'
SET @dateEnd = '${dateEnd}'

SET @FirstDistance = (SELECT TOP 1 b.distance 
      FROM ZebraDB_Log.dbo.log_msg a 
      LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b ON a.idx = b.ref_idx 
      WHERE a.veh_id = ${vehicleId}
      AND a.local_timestamp BETWEEN '${dateStart}' AND '${dateEnd}')

SELECT 
--ISNULL(e.evt_desc, 'LOCATION') AS event 
case 
      when isnull(e.evt_id,12) = 2 then 'SPEEDING'
      when isnull(e.evt_id,12) = 3 then 'IDLE'
      when isnull(e.evt_id,12) = 23 or ISNULL(ZebraDB.dbo.IsEngineOnTheLastByTime_Log(lm.veh_id ,lm.local_timestamp),0) = 0 then 'ENGINE OFF'
   when isnull(e.evt_id,12) = 22 then 'ENGINE ON'
      when e.evt_id not in (2,3,22,23) and isnull(lm.local_timestamp,GETDATE()-30) < GETDATE()-7 then 'OFFLINE'
      else 'NORMAL'
      end as Status
 
 , (lm.namt COLLATE Thai_CS_AS ) AS name
 , v.registration   as reg_no
 , lm.veh_id
 , CONVERT(VARCHAR(20),lm.local_timestamp,120) AS local_timestamp
 , lm.lat
 , lm.lon
 , lm.speed
 , lm.analog_level AS analog
 , ISNULL((SELECT TOP 1 max_fuel_voltage FROM model WHERE model_id = (SELECT TOP 1 model FROM vehicle WHERE veh_id = @vehicleId)),600) AS max_fuel_voltage
 , ISNULL((SELECT TOP 1 max_empty_voltage FROM model WHERE model_id = (SELECT TOP 1 model FROM vehicle WHERE veh_id = @vehicleId)),0) AS max_empty_voltage
 , ISNULL((SELECT TOP 1 max_fuel FROM model WHERE model_id = (SELECT TOP 1 model FROM vehicle WHERE veh_id = @vehicleId)),60) AS max_fuel
 , case when lmtag.tag_msg is null then  '-' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New( lm.veh_id,0, lm.local_timestamp) , '999.99','-') AS varchar) end AS Temp1
 , case when lmtag.tag_msg is null then  '-' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New( lm.veh_id,1, lm.local_timestamp) , '999.99','-') AS varchar) end AS Temp2
 , case when lmtag.tag_msg is null then  '-' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New( lm.veh_id,2, lm.local_timestamp) , '999.99','-') AS varchar) end AS Temp3
 , case when lmtag.tag_msg is null then  '-' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New( lm.veh_id,3, lm.local_timestamp) , '999.99','-') AS varchar) end AS Temp4
 , ld.distance - @FirstDistance AS distance
 , (SELECT COUNT(temperature)FROM ZebraDB_Log..log_temperature WHERE veh_id = @vehicleId  
        and local_timestamp BETWEEN @dateStart AND @dateEnd 
        and temperature < 1000 
        and temp_id = 0) AS Count_Temp1
, (SELECT COUNT(temperature)FROM ZebraDB_Log..log_temperature WHERE veh_id = @vehicleId  
        and local_timestamp BETWEEN @dateStart AND @dateEnd 
        and temperature < 1000 
        and temp_id = 1) AS Count_Temp2
, (SELECT COUNT(temperature)FROM ZebraDB_Log..log_temperature WHERE veh_id = @vehicleId  
        and local_timestamp BETWEEN @dateStart AND @dateEnd 
        and temperature < 1000 
        and temp_id = 2) AS Count_Temp3
, (SELECT COUNT(temperature)FROM ZebraDB_Log..log_temperature WHERE veh_id = @vehicleId  
        and local_timestamp BETWEEN @dateStart AND @dateEnd 
        and temperature < 1000 
        and temp_id = 3) AS Count_Temp4
FROM  ZebraDB_Log.dbo.log_msg lm
 LEFT JOIN ZebraDB.dbo.type_of_msg lmt           ON lm.type_of_msg = lmt.type_of_msg_id 
 LEFT JOIN ZebraDB_Log.dbo.log_msg_evt le         ON  lm.idx = le.ref_idx 
 LEFT JOIN ZebraDB_Log.dbo.log_msg_analog la  ON lm.idx = la.ref_idx
 LEFT JOIN ZebraDB.dbo.event e                  ON  e.evt_id = le.evt_id
 LEFT JOIN ZebraDB.dbo.vehicle v                 ON  lm.veh_id = v.veh_id
 LEFT JOIN ZebraDB_Log.dbo.log_msg_distance ld       ON  ld.ref_idx = lm.idx
 LEFT JOIN ZebraDB_Log.dbo.log_msg_tag lmtag   ON  lmtag.ref_idx = lm.idx 
WHERE lm.veh_id = ${vehicleId} AND lm.local_timestamp BETWEEN '${dateStart}' AND '${dateEnd}'
    `);
        return res.status(200).json(result.recordset);
      } catch (err) {
        console.log(err);
        next(err);
      }
    }
  }
);

module.exports = router;
