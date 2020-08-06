import mariadb
import numpy

class DataWareHouse:
    def __init__(self, apikey):
        self.con = mariadb.connect(
                user="xdmod-vpn-ro",
                password=apikey,
                host="openxdmod-dev-db.ccr.xdmod.org",
                port=3306,
                database="modw_aggregates")

    def aggregate(self, realm, groupby, statistic, start, end):
        cur = self.con.cursor()
        cur.execute("SELECT jf.application_id, SUM(jf.job_count) AS job_count, SUM(jf.cpu_time) / 3600.0 AS cpu_time FROM modw_aggregates.supremmfact_by_day jf, modw.days d where d.id= jf.day_id and d.day_start BETWEEN '2020-07-01' and '2020-07-31' GROUP BY 1 ORDER BY 3 DESC;")

        data = []
        for res in cur:
            data.append(res[2])

        return numpy.array(data)
