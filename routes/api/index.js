const express = require("express");
const router = express.Router();
const { pool_zb, pool_zb_report } = require("../../config/db");
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

SET @FirstDistance = (select top 1 c_ld.distance as distance 
  from ZebraDB_Log.dbo.log_msg c_lm
  LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
  WHERE c_lm.veh_id = ${vehicleId}
      AND c_lm.local_timestamp BETWEEN '${dateStart}' AND '${dateEnd}')

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

/* GET api/vehicle/registration */
router.get("/vehicle/registration/:fleetId", isAuth, async (req, res, next) => {
  const { fleetId } = req.params;
  if (!fleetId) {
    return next(ApiError.badRequest("Invalid credentials!"));
  } else {
    try {
      const pool_zbq = await pool_zb();

      const result = await pool_zbq.query(`
        select fv.veh_id
        , v.registration
    from ZebraDB.dbo.fleet_vehicle fv
    left join ZebraDB.dbo.vehicle v			on v.veh_id = fv.veh_id
    where fv.fleet_id = ${fleetId}
    `);

      return res.status(200).json(result.recordset);
    } catch (err) {
      console.log(err);
      next(err);
    }
  }
});

// oil report by fleet
/* GET api/vehicle/ */
router.get(
  "/vehicle/registration/:fleetId/:dateStart/:dateEnd",
  isAuth,
  async (req, res, next) => {
    const { fleetId, dateStart, dateEnd } = req.params;
    if (!fleetId) {
      return next(ApiError.badRequest("Invalid credentials!"));
    } else {
      try {
        const pool_zbq = await pool_zb();
        const pool_zbrq = await pool_zb_report();
        // fleet --> VehID
        const result = await pool_zbq.query(`
        select fv.veh_id
        , v.registration
    from ZebraDB.dbo.fleet_vehicle fv
    left join ZebraDB.dbo.vehicle v			on v.veh_id = fv.veh_id
    where fv.fleet_id = ${fleetId}
    `);
        console.log(result.recordset);
        var result2;
        var output = [];
        for (var i = 0; i < result.recordset.length; i++) {
          if (!result.recordset || !dateStart || !dateEnd) {
            return next(ApiError.badRequest("Invalid credentials!"));
          } else {
            try {
              var vID = result.recordset[i].veh_id;
              console.log(vID);

              //EnginOn
              const result2 = await pool_zbq.query(`
                    SELECT ISNULL(a_e.evt_desc, 'LOCATION') AS event
                    , a_lm.veh_id
                        , CONVERT(VARCHAR(20),a_lm.local_timestamp,120) AS local_timestamp
                        , (select top 1 c_ld.distance as distance
                            from ZebraDB_Log.dbo.log_msg c_lm
                            LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
                          WHERE c_lm.veh_id = ${vID}
                            AND c_lm.local_timestamp <= (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
                            AND c_ld.distance is not null
                            order by c_lm.local_timestamp desc)
                            as distance
                        , (select dbo.GetEngineOff_FuelLevel(a_lm.veh_id,a_lm.local_timestamp)) as oil_off
                        , (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)) as time_off
                        , (select top 1 b_ld.distance as distance
                              from ZebraDB_Log.dbo.log_msg b_lm
                              LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b_ld     ON  b_ld.ref_idx = b_lm.idx
                          WHERE b_lm.veh_id = ${vID}
                              AND b_lm.local_timestamp <= (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
                              AND b_ld.distance is not null
                              order by b_lm.local_timestamp desc) as distance_off
                        , (SELECT CASE WHEN EXISTS
                            (SELECT top 1 c_lm.idx
                              FROM	ZebraDB_Log.dbo.log_msg c_lm
                              WHERE  c_lm.veh_id = ${vID} AND c_lm.type_of_msg = 8
                                AND c_lm.local_timestamp BETWEEN (select DATEADD(minute,-15,(select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)))) AND (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
                            )

                            THEN '1'
                            ELSE '0'
                          END) AS resetEvent

                    FROM	ZebraDB_Log.dbo.log_msg a_lm
                    LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le			ON  a_lm.idx = a_le.ref_idx
                    LEFT JOIN ZebraDB.dbo.event a_e						ON  a_e.evt_id = a_le.evt_id
                    LEFT JOIN ZebraDB.dbo.vehicle a_v					ON  a_lm.veh_id = a_v.veh_id
                    LEFT JOIN ZebraDB.dbo.model a_m						ON a_m.model_id = a_v.model
                    LEFT JOIN ZebraDB_Log.dbo.log_msg_distance a_ld     ON  a_ld.ref_idx = a_lm.idx
                      WHERE a_lm.veh_id = ${vID}
                      AND a_lm.local_timestamp BETWEEN '${dateStart}' AND '${dateEnd}'
                      AND a_le.evt_id = 22
                  `);

              const result4 = await pool_zbq.query(`
        SELECT *
FROM (SELECT  top 1
		  a_lm.local_timestamp as timeOff
		, (select dbo.GetEngineOff_FuelLevel(a_lm.veh_id,a_lm.local_timestamp)) as oil
		, (select top 1 c_ld.distance as distance
			from ZebraDB_Log.dbo.log_msg c_lm
			LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
			WHERE c_lm.veh_id = ${vID} 
			AND c_lm.local_timestamp <= a_lm.local_timestamp 
			AND c_ld.distance is not null
			order by c_lm.local_timestamp desc)
			 as distance
FROM	ZebraDB_Log.dbo.log_msg a_lm
		LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le			ON  a_lm.idx = a_le.ref_idx        
        LEFT JOIN ZebraDB.dbo.vehicle a_v					ON  a_lm.veh_id = a_v.veh_id
		LEFT JOIN ZebraDB.dbo.model a_m						ON a_m.model_id = a_v.model
        LEFT JOIN ZebraDB_Log.dbo.log_msg_distance a_ld     ON  a_ld.ref_idx = a_lm.idx                            
		WHERE a_lm.veh_id = ${vID} 
		AND a_lm.local_timestamp between '${dateStart}' AND '${dateEnd}'
		AND a_le.evt_id = 22
		order by a_lm.local_timestamp) first

UNION ALL

SELECT * 
FROM (SELECT top 1
		  b_lm.local_timestamp as timeOff
		, ISNULL(dbo.CalculateFuelLevel(b_m.max_empty_voltage, b_m.max_fuel_voltage, b_m.max_fuel, b_lm.analog_level), '-') as oil
		, (select top 1 c_ld.distance as distance
			from ZebraDB_Log.dbo.log_msg c_lm
			LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
			WHERE c_lm.veh_id = ${vID}
			AND c_lm.local_timestamp <= b_lm.local_timestamp 
			AND c_ld.distance is not null
			order by c_lm.local_timestamp desc)
			 as distance
FROM	ZebraDB_Log.dbo.log_msg b_lm
		LEFT JOIN ZebraDB_Log.dbo.log_msg_evt b_le			ON  b_lm.idx = b_le.ref_idx 
        LEFT JOIN ZebraDB.dbo.vehicle b_v					ON  b_lm.veh_id = b_v.veh_id
		LEFT JOIN ZebraDB.dbo.model b_m						ON  b_m.model_id = b_v.model
        LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b_ld     ON  b_ld.ref_idx = b_lm.idx                            
		WHERE b_lm.veh_id = ${vID} 
		AND b_lm.local_timestamp between '${dateStart}' AND '${dateEnd}'
        AND b_le.evt_id = 23
		ORDER BY b_lm.local_timestamp DESC) last
                  `);

              const result5 = await pool_zbrq.query(`
              select o.veh_type, o.value
              , ov.veh_id
              
        from ZebraDB_Report.dbo.oil_veh_vehType ov
        left join ZebraDB_Report.dbo.oil_vehicleType o			on o.vehTypeId = ov.vehtype_id
        where ov.veh_id = ${vID}
                  `);

              var arr = [];

              if (result2.recordset.length < 1) {
                // No Engine on
                arr.push({
                  local_timestamp: 0,
                  maxOil: 0,
                  distance: 0,
                  time_off: 0,
                  oil_off: 0,
                  distance_off: 0,
                  veh_id: vID,
                  timeStart: 0,
                  oilStart: 0,
                  disStart: 0,
                  timeEnd: 0,
                  oilEnd: 0,
                  disEnd: 0,
                  registration: result.recordset[i].registration,
                  veh_type:
                    result5.recordset.length < 1
                      ? 0
                      : result5.recordset[0].veh_type,
                  veh_type_value:
                    result5.recordset.length < 1
                      ? 0
                      : result5.recordset[0].value,
                });
              } else {
                for (var j = 0; j < result2.recordset.length; j++) {
                  if (!result2.recordset) {
                    return next(ApiError.badRequest("Invalid credentials!"));
                  } else {
                    try {
                      var dateEnginOn = result2.recordset[j].local_timestamp;
                      var resetEvent = result2.recordset[j].resetEvent;
                      var oilOff = parseInt(result2.recordset[j].oil_off);

                      //EnginOn to EnginOff
                      const result3 = await pool_zbq.query(`
                                    SELECT a_lm.local_timestamp
                                          ,a_lm.idx
                                          ,ISNULL(dbo.CalculateFuelLevel(a_m.max_empty_voltage, a_m.max_fuel_voltage, a_m.max_fuel, a_lm.analog_level), '-') as oil
  
                                    FROM	ZebraDB_Log.dbo.log_msg a_lm
                                    LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le      ON  a_lm.idx = a_le.ref_idx
                                    LEFT JOIN ZebraDB.dbo.vehicle a_v               ON a_lm.veh_id = a_v.veh_id
                                    LEFT JOIN ZebraDB.dbo.model a_m					ON a_m.model_id = a_v.model
                                    
  
                                          WHERE a_lm.veh_id = ${vID}
                                          
                                          AND a_lm.local_timestamp between '${dateEnginOn}'  AND (select dbo.GetEngineOff_BehindTime(${vID},'${dateEnginOn}'))
                                          AND (a_le.evt_id not in (22,23) OR a_le.evt_id is null)
                `);
                      if (resetEvent == "0") {
                        var max = 0;
                        var arrs = [];
                        for (var a of result3.recordset) {
                          arrs.push(a.oil);
                        }
                        max = Math.max(
                          ...arrs.map((item) => (isNaN(+item) ? 0 : +item))
                        );

                        if (result3.recordset.length > 5 && max - oilOff > 20) {
                          arr.push({
                            local_timestamp: dateEnginOn,
                            maxOil: max,
                            distance: result2.recordset[j].distance,
                            time_off: result2.recordset[j].time_off,
                            oil_off: oilOff,
                            distance_off: result2.recordset[j].distance_off,
                            veh_id: vID,
                            timeStart: result4.recordset[0].timeOff,
                            oilStart: result4.recordset[0].oil,
                            disStart: result4.recordset[0].distance,
                            timeEnd: result4.recordset[1].timeOff,
                            oilEnd: result4.recordset[1].oil,
                            disEnd: result4.recordset[1].distance,
                            registration: result.recordset[i].registration,
                            veh_type:
                              result5.recordset.length < 1
                                ? 0
                                : result5.recordset[0].veh_type,
                            veh_type_value:
                              result5.recordset.length < 1
                                ? 0
                                : result5.recordset[0].value,
                          });
                        } else {
                          arr.push({
                            local_timestamp: 0,
                            maxOil: 0,
                            distance: 0,
                            time_off: 0,
                            oil_off: 0,
                            distance_off: 0,
                            veh_id: vID,
                            timeStart: result4.recordset[0].timeOff,
                            oilStart: result4.recordset[0].oil,
                            disStart: result4.recordset[0].distance,
                            timeEnd: result4.recordset[1].timeOff,
                            oilEnd: result4.recordset[1].oil,
                            disEnd: result4.recordset[1].distance,
                            registration: result.recordset[i].registration,
                            veh_type:
                              result5.recordset.length < 1
                                ? 0
                                : result5.recordset[0].veh_type,
                            veh_type_value:
                              result5.recordset.length < 1
                                ? 0
                                : result5.recordset[0].value,
                          });
                        }
                      }

                      console.log(vID);
                    } catch (err) {
                      console.log(err);
                      next(err);
                    }
                  }
                }
              }

              output.push(arr);
              console.log("finish " + result.recordset[i].veh_id);
            } catch (err) {
              console.log(err);
              next(err);
            }
          }
        }
        return res.status(200).json(output);
      } catch (err) {
        console.log(err);
        next(err);
      }
    }
  }
);

// oil report by vehicle
/* GET api/vehicle/ */
router.get(
  "/vehicle/oilreportByVehicle/:vehicleId/:dateStart/:dateEnd",
  isAuth,
  async (req, res, next) => {
    const { vehicleId, dateStart, dateEnd } = req.params;
    if (!vehicleId || !dateStart || !dateEnd) {
      return next(ApiError.badRequest("Invalid credentials!"));
    } else {
      try {
        const pool_zbq = await pool_zb();
        const pool_zbrq = await pool_zb_report();
        var output = [];
        //EnginOn
        const result2 = await pool_zbq.query(`
              SELECT ISNULL(a_e.evt_desc, 'LOCATION') AS event
              , a_lm.veh_id
                  , CONVERT(VARCHAR(20),a_lm.local_timestamp,120) AS local_timestamp
                  , (select top 1 c_ld.distance as distance
                      from ZebraDB_Log.dbo.log_msg c_lm
                      LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
                    WHERE c_lm.veh_id = ${vehicleId}
                      AND c_lm.local_timestamp <= (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
                      AND c_ld.distance is not null
                      order by c_lm.local_timestamp desc)
                      as distance
                  , (select dbo.GetEngineOff_FuelLevel(a_lm.veh_id,a_lm.local_timestamp)) as oil_off
                  , (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)) as time_off
                  , (select top 1 b_ld.distance as distance
                        from ZebraDB_Log.dbo.log_msg b_lm
                        LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b_ld     ON  b_ld.ref_idx = b_lm.idx
                    WHERE b_lm.veh_id = ${vehicleId}
                        AND b_lm.local_timestamp <= (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
                        AND b_ld.distance is not null
                        order by b_lm.local_timestamp desc) as distance_off
                  , (SELECT CASE WHEN EXISTS
                      (SELECT top 1 c_lm.idx
                        FROM	ZebraDB_Log.dbo.log_msg c_lm
                        WHERE  c_lm.veh_id = ${vehicleId} AND c_lm.type_of_msg = 8
                          AND c_lm.local_timestamp BETWEEN (select DATEADD(minute,-15,(select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)))) AND (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
                      )

                      THEN '1'
                      ELSE '0'
                    END) AS resetEvent

              FROM	ZebraDB_Log.dbo.log_msg a_lm
              LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le			ON  a_lm.idx = a_le.ref_idx
              LEFT JOIN ZebraDB.dbo.event a_e						ON  a_e.evt_id = a_le.evt_id
              LEFT JOIN ZebraDB.dbo.vehicle a_v					ON  a_lm.veh_id = a_v.veh_id
              LEFT JOIN ZebraDB.dbo.model a_m						ON a_m.model_id = a_v.model
              LEFT JOIN ZebraDB_Log.dbo.log_msg_distance a_ld     ON  a_ld.ref_idx = a_lm.idx
                WHERE a_lm.veh_id = ${vehicleId}
                AND a_lm.local_timestamp BETWEEN '${dateStart}' AND '${dateEnd}'
                AND a_le.evt_id = 22
            `);

        const result4 = await pool_zbq.query(`
  SELECT *
FROM (SELECT  top 1
a_lm.local_timestamp as timeOff
, (select dbo.GetEngineOff_FuelLevel(a_lm.veh_id,a_lm.local_timestamp)) as oil
, (select top 1 c_ld.distance as distance
from ZebraDB_Log.dbo.log_msg c_lm
LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
WHERE c_lm.veh_id = ${vehicleId} 
AND c_lm.local_timestamp <= a_lm.local_timestamp 
AND c_ld.distance is not null
order by c_lm.local_timestamp desc)
 as distance
FROM	ZebraDB_Log.dbo.log_msg a_lm
LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le			ON  a_lm.idx = a_le.ref_idx        
  LEFT JOIN ZebraDB.dbo.vehicle a_v					ON  a_lm.veh_id = a_v.veh_id
LEFT JOIN ZebraDB.dbo.model a_m						ON a_m.model_id = a_v.model
  LEFT JOIN ZebraDB_Log.dbo.log_msg_distance a_ld     ON  a_ld.ref_idx = a_lm.idx                            
WHERE a_lm.veh_id = ${vehicleId} 
AND a_lm.local_timestamp between '${dateStart}' AND '${dateEnd}'
AND a_le.evt_id = 22
order by a_lm.local_timestamp) first

UNION ALL

SELECT * 
FROM (SELECT top 1
b_lm.local_timestamp as timeOff
, ISNULL(dbo.CalculateFuelLevel(b_m.max_empty_voltage, b_m.max_fuel_voltage, b_m.max_fuel, b_lm.analog_level), '-') as oil
, (select top 1 c_ld.distance as distance
from ZebraDB_Log.dbo.log_msg c_lm
LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
WHERE c_lm.veh_id = ${vehicleId}
AND c_lm.local_timestamp <= b_lm.local_timestamp 
AND c_ld.distance is not null
order by c_lm.local_timestamp desc)
 as distance
FROM	ZebraDB_Log.dbo.log_msg b_lm
LEFT JOIN ZebraDB_Log.dbo.log_msg_evt b_le			ON  b_lm.idx = b_le.ref_idx 
  LEFT JOIN ZebraDB.dbo.vehicle b_v					ON  b_lm.veh_id = b_v.veh_id
LEFT JOIN ZebraDB.dbo.model b_m						ON  b_m.model_id = b_v.model
  LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b_ld     ON  b_ld.ref_idx = b_lm.idx                            
WHERE b_lm.veh_id = ${vehicleId} 
AND b_lm.local_timestamp between '${dateStart}' AND '${dateEnd}'
  AND b_le.evt_id = 23
ORDER BY b_lm.local_timestamp DESC) last
`);

        const result5 = await pool_zbrq.query(`
              select o.veh_type, o.value
              , ov.veh_id
              
        from ZebraDB_Report.dbo.oil_veh_vehType ov
        left join ZebraDB_Report.dbo.oil_vehicleType o			on o.vehTypeId = ov.vehtype_id
        where ov.veh_id = ${vID}
                  `);

        var arr = [];
        for (var j = 0; j < result2.recordset.length; j++) {
          if (!result2.recordset) {
            return next(ApiError.badRequest("Invalid credentials!"));
          } else {
            try {
              var dateEnginOn = result2.recordset[j].local_timestamp;
              var resetEvent = result2.recordset[j].resetEvent;
              var oilOff = parseInt(result2.recordset[j].oil_off);

              //EnginOn to EnginOff
              const result3 = await pool_zbq.query(`
                            SELECT a_lm.local_timestamp
                                  ,a_lm.idx
                                  ,ISNULL(dbo.CalculateFuelLevel(a_m.max_empty_voltage, a_m.max_fuel_voltage, a_m.max_fuel, a_lm.analog_level), '-') as oil

                            FROM	ZebraDB_Log.dbo.log_msg a_lm
                            LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le      ON  a_lm.idx = a_le.ref_idx
                            LEFT JOIN ZebraDB.dbo.vehicle a_v               ON a_lm.veh_id = a_v.veh_id
                            LEFT JOIN ZebraDB.dbo.model a_m					ON a_m.model_id = a_v.model

                                  WHERE a_lm.veh_id = ${vehicleId}
                                  AND a_lm.local_timestamp between '${dateEnginOn}'  AND (select dbo.GetEngineOff_BehindTime(${vehicleId},'${dateEnginOn}'))
                                  AND (a_le.evt_id not in (22,23) OR a_le.evt_id is null)
        `);
              if (resetEvent == "0") {
                var max = 0;
                var arrs = [];
                // const n = arr.values();
                for (var a of result3.recordset) {
                  arrs.push(a.oil);
                }
                max = Math.max(
                  ...arrs.map((item) => (isNaN(+item) ? 0 : +item))
                );

                if (result3.recordset.length > 5) {
                  if (max - oilOff > 20) {
                    arr.push({
                      local_timestamp: dateEnginOn,
                      maxOil: max,
                      distance: result2.recordset[j].distance,
                      time_off: result2.recordset[j].time_off,
                      oil_off: oilOff,
                      distance_off: result2.recordset[j].distance_off,
                      veh_id: vehicleId,
                      timeStart: result4.recordset[0].timeOff,
                      oilStart: result4.recordset[0].oil,
                      disStart: result4.recordset[0].distance,
                      timeEnd: result4.recordset[1].timeOff,
                      oilEnd: result4.recordset[1].oil,
                      disEnd: result4.recordset[1].distance,
                      veh_type: result5.recordset[0].veh_type,
                      veh_type_value: result5.recordset[0].value,
                    });
                  }
                }
              }
            } catch (err) {
              console.log(err);
              next(err);
            }
          }
        }
        return res.status(200).json(arr);
      } catch (err) {
        console.log(err);
        next(err);
      }
    }
  }
);

/* GET api/vehicleLog/firstAndLastRow */
router.get(
  "/vehicleLog/firstAndLastRow/:vehicleId/:dateStart/:dateEnd",
  isAuth,
  async (req, res, next) => {
    const { vehicleId, dateStart, dateEnd } = req.params;
    if (!vehicleId || !dateStart || !dateEnd) {
      return next(ApiError.badRequest("Invalid credentials!"));
    } else {
      try {
        const pool_zbq = await pool_zb();

        const result = await pool_zbq.query(`
        SELECT *
FROM (SELECT  top 1
		  a_lm.local_timestamp as timeOff
		, (select dbo.GetEngineOff_FuelLevel(a_lm.veh_id,a_lm.local_timestamp)) as oil
		, (select top 1 c_ld.distance as distance
			from ZebraDB_Log.dbo.log_msg c_lm
			LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
			WHERE c_lm.veh_id = ${vehicleId} 
			AND c_lm.local_timestamp <= a_lm.local_timestamp 
			AND c_ld.distance is not null
			order by c_lm.local_timestamp desc)
			 as distance
FROM	ZebraDB_Log.dbo.log_msg a_lm
		LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le			ON  a_lm.idx = a_le.ref_idx        
        LEFT JOIN ZebraDB.dbo.vehicle a_v					ON  a_lm.veh_id = a_v.veh_id
		LEFT JOIN ZebraDB.dbo.model a_m						ON a_m.model_id = a_v.model
        LEFT JOIN ZebraDB_Log.dbo.log_msg_distance a_ld     ON  a_ld.ref_idx = a_lm.idx                            
		WHERE a_lm.veh_id = ${vehicleId} 
		AND a_lm.local_timestamp between '${dateStart}' AND '${dateEnd}'
		AND a_le.evt_id = 22
		order by a_lm.local_timestamp) first

UNION ALL

SELECT * 
FROM (SELECT top 1
		  b_lm.local_timestamp as timeOff
		, ISNULL(dbo.CalculateFuelLevel(b_m.max_empty_voltage, b_m.max_fuel_voltage, b_m.max_fuel, b_lm.analog_level), '-') as oil
		, (select top 1 c_ld.distance as distance
			from ZebraDB_Log.dbo.log_msg c_lm
			LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
			WHERE c_lm.veh_id = ${vehicleId}
			AND c_lm.local_timestamp <= b_lm.local_timestamp 
			AND c_ld.distance is not null
			order by c_lm.local_timestamp desc)
			 as distance
FROM	ZebraDB_Log.dbo.log_msg b_lm
		LEFT JOIN ZebraDB_Log.dbo.log_msg_evt b_le			ON  b_lm.idx = b_le.ref_idx 
        LEFT JOIN ZebraDB.dbo.vehicle b_v					ON  b_lm.veh_id = b_v.veh_id
		LEFT JOIN ZebraDB.dbo.model b_m						ON  b_m.model_id = b_v.model
        LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b_ld     ON  b_ld.ref_idx = b_lm.idx                            
		WHERE b_lm.veh_id = ${vehicleId} 
		AND b_lm.local_timestamp between '${dateStart}' AND '${dateEnd}'
        AND b_le.evt_id = 23
		ORDER BY b_lm.local_timestamp DESC) last
    `);
        return res.status(200).json(result.recordset);
      } catch (err) {
        console.log(err);
        next(err);
      }
    }
  }
);

/* GET api/vehicleLog/timeEngineOff */
router.get(
  "/vehicleLog/timeEngineOff/:vehicleId/:dateStart",
  isAuth,
  async (req, res, next) => {
    const { vehicleId, dateStart } = req.params;
    // console.log(vehicleId);
    // console.log(dateStart);

    if (!vehicleId || !dateStart) {
      return next(ApiError.badRequest("Invalid credentials!"));
    } else {
      try {
        const pool_zbq = await pool_zb();

        const result = await pool_zbq.query(`
        SELECT 																
		a_lm.local_timestamp
		,a_lm.idx
		,ISNULL(dbo.CalculateFuelLevel(a_m.max_empty_voltage, a_m.max_fuel_voltage, a_m.max_fuel, a_lm.analog_level), '-') as oil		

FROM	ZebraDB_Log.dbo.log_msg a_lm
		LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le      ON  a_lm.idx = a_le.ref_idx                             
		LEFT JOIN ZebraDB.dbo.vehicle a_v               ON a_lm.veh_id = a_v.veh_id
		LEFT JOIN ZebraDB.dbo.model a_m					ON a_m.model_id = a_v.model                                                        

		WHERE a_lm.veh_id = ${vehicleId} 
		AND a_lm.local_timestamp between '${dateStart}'  AND (select dbo.GetEngineOff_BehindTime(${vehicleId},'${dateStart}'))
		AND (a_le.evt_id not in (22,23) OR a_le.evt_id is null)
    `);
        return res.status(200).json(result.recordset);
      } catch (err) {
        console.log(err);
        next(err);
      }
    }
  }
);

/* GET api/vehicleLog/logMSG */
router.get(
  "/vehicleLog/logMSG/:vehicleId/:dateStart/:dateEnd",
  isAuth,
  async (req, res, next) => {
    const { vehicleId, dateStart, dateEnd } = req.params;

    if (!vehicleId || !dateStart || !dateEnd) {
      return next(ApiError.badRequest("Invalid credentials!"));
    } else {
      try {
        const pool_zbq = await pool_zb();

        const result = await pool_zbq.query(`
        SELECT ISNULL(a_e.evt_desc, 'LOCATION') AS event 	
		, CONVERT(VARCHAR(20),a_lm.local_timestamp,120) AS local_timestamp
		, (select top 1 c_ld.distance as distance
			from ZebraDB_Log.dbo.log_msg c_lm
			LEFT JOIN ZebraDB_Log.dbo.log_msg_distance c_ld     ON  c_ld.ref_idx = c_lm.idx
			WHERE c_lm.veh_id = ${vehicleId}
			AND c_lm.local_timestamp <= (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)) 
			AND c_ld.distance is not null
			order by c_lm.local_timestamp desc)
			 as distance
		,(select dbo.GetEngineOff_FuelLevel(a_lm.veh_id,a_lm.local_timestamp)) as oil_off
		, (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)) as time_off
		, (select top 1 b_ld.distance as distance
			from ZebraDB_Log.dbo.log_msg b_lm
			LEFT JOIN ZebraDB_Log.dbo.log_msg_distance b_ld     ON  b_ld.ref_idx = b_lm.idx
			WHERE b_lm.veh_id = ${vehicleId} 
			AND b_lm.local_timestamp <= (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)) 
			AND b_ld.distance is not null
			order by b_lm.local_timestamp desc) as distance_off
		, (SELECT 
	CASE WHEN EXISTS 
	(
		SELECT top 1 c_lm.idx 
		FROM	ZebraDB_Log.dbo.log_msg c_lm
				WHERE  c_lm.veh_id = ${vehicleId} AND c_lm.type_of_msg = 8
				AND c_lm.local_timestamp BETWEEN (select DATEADD(minute,-15,(select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp)))) AND (select dbo.GetEngineOff_PreviousTime(a_lm.veh_id,a_lm.local_timestamp))
	)
	
	THEN '1'
    ELSE '0'
END) AS resetEvent
		
FROM	ZebraDB_Log.dbo.log_msg a_lm
		LEFT JOIN ZebraDB_Log.dbo.log_msg_evt a_le			ON  a_lm.idx = a_le.ref_idx 
        LEFT JOIN ZebraDB.dbo.event a_e						ON  a_e.evt_id = a_le.evt_id
        LEFT JOIN ZebraDB.dbo.vehicle a_v					ON  a_lm.veh_id = a_v.veh_id
		LEFT JOIN ZebraDB.dbo.model a_m						ON a_m.model_id = a_v.model
        LEFT JOIN ZebraDB_Log.dbo.log_msg_distance a_ld     ON  a_ld.ref_idx = a_lm.idx                            
		WHERE a_lm.veh_id = ${vehicleId} 
		AND a_lm.local_timestamp BETWEEN '${dateStart}' AND '${dateEnd}' 
		AND a_le.evt_id = 22
    `);
        return res.status(200).json(result.recordset);
      } catch (err) {
        console.log(err);
        next(err);
      }
    }
  }
);

//Oil report vehicle type --> zebra_report
/* GET api/vehicleLog/oilVehicleType */
router.get("/vehicleLog/oilVehicleType", isAuth, async (req, res, next) => {
  try {
    const pool_zbq = await pool_zb_report();

    const result = await pool_zbq.query(`
        SELECT * 
  FROM ZebraDB_Report.dbo.oil_vehicleType
    `);
    return res.status(200).json(result.recordset);
  } catch (err) {
    console.log(err);
    next(err);
  }
});

module.exports = router;
