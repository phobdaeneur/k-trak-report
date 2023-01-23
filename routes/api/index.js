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
      SELECT a.fleet_id
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
      when isnull(e.evt_id,12) = 2 then 'SPEEDING'
      when isnull(e.evt_id,12) = 3 then 'IDLE'
      when isnull(c.local_timestamp,GETDATE()-30) between (dateadd(hour,-24,getdate())) and (dateadd(hour,-12,getdate())) then 'NO SIGNAL 12Hr.'
      when isnull(c.local_timestamp,GETDATE()-30) < (dateadd(hour,-24,getdate())) then 'NO SIGNAL 24Hr.'
      when isnull(e.evt_id,12) not in (2,3) 
        and isnull(c.local_timestamp,GETDATE()-30) > (dateadd(hour,-12,getdate())) 
        and ISNULL(ZebraDB.dbo.IsEngineOnTheLastByTime_Log(a.veh_id ,c.local_timestamp),0) = 0 then 'ENGINE OFF'
      else 'NORMAL'
      end as Status
      ,case when f.tag_msg is null then  '?' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New(a.veh_id,0,c.local_timestamp) , '999.99','-') AS varchar) end AS Temp1
      ,case when f.tag_msg is null then  '?' else  CAST(REPLACE( dbo.GetLastTemperature_LogCur_New(a.veh_id,1,c.local_timestamp) , '999.99','-') AS varchar) end AS Temp2
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

module.exports = router;
