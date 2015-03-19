using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySql.Data.Types;
using MySql.Data;
using System.Data.SqlClient;
using System.Transactions;
using Microsoft.SqlServer;
using Microsoft.SqlServer.Types;

namespace CopyAndTransform
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("hello this will copy and transform ");


            EvilTransform.Transform transformer = new EvilTransform.Transform();
            EvilTransform.PointLatLng WGSpoint = new EvilTransform.PointLatLng();

            int finished = 0;
            int step = 100000;

            while (true)
            {
                Console.Write(DateTime.Now + "  " + (finished).ToString());


                ////Get Harvester AppKeys
                //Coordiante<Int64, double, double> CList = SQLServer.GetCoordinates((int)(finished * 1000000));
                //int c = CList.Count();
                //Console.Write(" " + DateTime.Now + " got " + c + " records.");
                //if (c == 0)
                //{
                //    Console.WriteLine("SQL does not deliver any records!!");

                //    continue;
                //}
                //int x = CList.Count();

                DataTable mytable = new DataTable();
                //mytable = SQLServer.GetFull100000NBT2table();
                mytable = SQLServer.READDataTableFromSQLServer(finished, step);

                //Console.WriteLine(mytable.Columns[38].DataType.Name.ToString());
                mytable.Columns.Remove("location");
                mytable.Columns.Add("location", typeof(SqlGeography));
                
                //Modify datatable
                foreach (DataRow row in mytable.Rows)
                {
                    //Console.WriteLine("  ");
                    //Console.WriteLine(row[15].ToString() + " " + row[16].ToString());
                    double badLat = double.Parse(row[15].ToString());
                    double badLog = double.Parse(row[16].ToString());

                    WGSpoint = transformer.GCJ2WGSExact(badLat, badLog);
                    
                    row.SetField("WGSLatitudeX", WGSpoint.Lat);
                    row.SetField("WGSLongitudeY", WGSpoint.Lng);
                    //DataRow newRow = mytable.NewRow();
                    //newRow["location"] = SqlGeography.Point(55.55, 66.66, 4326);
                    row.SetField("location", SqlGeography.Point(WGSpoint.Lat, WGSpoint.Lng, 4326));
                }


                //Output datatable
                //////////////foreach (DataRow row in mytable.Rows)
                //////////////{;
                //////////////    Console.WriteLine();
                //////////////    for (int i = 0; i < mytable.Columns.Count; i++)
                //////////////    {
                //////////////        Console.Write(row[i].ToString() + " ");
                //////////////    }
                //////////////}


                SQLServer.WRITEDataTableToSQLServer("NBT2_transformed", mytable, "weiboDEV");

                finished = finished + step;

                Console.Write(" finished\n");

                //for (int i = 0; i < x; i++)
                //{

                //    Console.WriteLine(i.ToString() + "\t" +
                //        CList[i].Item1 + "\t" +
                //                      CList[i].Item2 + "\t" +
                //                      CList[i].Item3
                //                      );

                //}
                //Console.WriteLine(x.ToString()+ " Records.");

                //////////CONNECT
                ////////SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
                ////////try
                ////////{
                ////////    myConnection.Open();
                ////////}
                ////////catch (Exception e)
                ////////{
                ////////    Console.WriteLine(e.ToString());
                ////////}



                ////UPDATE
                //try
                //{
                //    StringBuilder sql = new StringBuilder();
                //    int batchSize = 100;
                //    int currentBatchCount = 0;
                //    //SqlCommand cmd = null;
                //    for (int i = 0; i < CList.Count(); i++)
                //    {
                //        switch (i)
                //        {
                //            case 0:
                //                {
                //                    Console.Write(" 0");
                //                    break;
                //                }
                //            case 10000:
                //                {
                //                    Console.Write(".");
                //                    break;
                //                }
                //            case 100000 - 1:
                //                {
                //                    Console.Write("..10");
                //                    break;
                //                }
                //            case 200000 - 1:
                //                {
                //                    Console.Write("..20");
                //                    break;
                //                }
                //            case 300000 - 1:
                //                {
                //                    Console.Write("..30");
                //                    break;
                //                }
                //            case 400000 - 1:
                //                {
                //                    Console.Write("..40");
                //                    break;
                //                }
                //            case 500000 - 1:
                //                {
                //                    Console.Write("..50");
                //                    break;
                //                }
                //            case 600000 - 1:
                //                {
                //                    Console.Write("..60");
                //                    break;
                //                }
                //            case 700000 - 1:
                //                {
                //                    Console.Write("..70");
                //                    break;
                //                }
                //            case 800000 - 1:
                //                {
                //                    Console.Write("..80");
                //                    break;
                //                }
                //            case 900000 - 1:
                //                {
                //                    Console.Write("..90");
                //                    break;
                //                }
                //            case 1000000 - 1:
                //                {
                //                    Console.Write("..100");
                //                    break;
                //                }
                //            default:
                //                {
                //                    break;
                //                }
                //        }
                //        double badLat = CList[i].Item2;
                //        double badLog = CList[i].Item3;
                       // WGSpoint = transformer.GCJ2WGSExact(badLat, badLog);





                        //string s = "UPDATE TOP(1) [weiboDEV].[dbo].[NBT2_trans] set [WGSLatitudeX]=" + WGSpoint.Lat + ", [WGSLongitudeY]=" + WGSpoint.Lng + ", [location]=geography::STPointFromText('POINT (" + WGSpoint.Lng + " " + WGSpoint.Lat + ")',4326) WHERE [idNearByTimeLine]=" + CList[i].Item1 + ";";
                        //Console.WriteLine(i.ToString());
                        //Console.WriteLine(s);

                        //sql.AppendFormat("UPDATE [weiboDEV].[dbo].[NBT2_trans] set [WGSLatitudeX]={0}, [WGSLongitudeY]={1}, [location]=geography::STPointFromText('POINT ({2} {3})',4326) WHERE [idNearByTimeLine]={4};", WGSpoint.Lat, WGSpoint.Lng, WGSpoint.Lng, WGSpoint.Lat, CList[i].Item1);



                        //SqlCommand update = new SqlCommand(s, myConnection);
                        //update.CommandTimeout = 600;
                        //update.ExecuteNonQuery();


                       // currentBatchCount++;
                        //Console.WriteLine(currentBatchCount);
                        //Console.WriteLine(sql.ToString());
                        //if (currentBatchCount >= batchSize)
                        //{
                        //    //Console.WriteLine(sql.ToString());
                        //    //Console.WriteLine(currentBatchCount.ToString());

                        //    SqlCommand update = new SqlCommand(sql.ToString(), myConnection);
                        //    //cmd.CommandText = sql.ToString();
                        //    update.CommandTimeout = 120;
                        //    update.ExecuteNonQuery();
                        //    sql.Clear();
                        //    Console.WriteLine(sql.ToString());
                        //    //sql = new StringBuilder();
                        //    currentBatchCount = 0;
                        //}




                        //// Create a SqlDataAdapter.
                        //SqlDataAdapter adapter = new SqlDataAdapter();

                        //// Set the UPDATE command and parameters.
                        //adapter.UpdateCommand = new SqlCommand("UPDATE Production.ProductCategory SET " + "Name=@Name WHERE ProductCategoryID=@ProdCatID;", connection);
                        //adapter.UpdateCommand.Parameters.Add("@Name", SqlDbType.NVarChar, 50, "Name");
                        //adapter.UpdateCommand.Parameters.Add("@ProdCatID", SqlDbType.Int, 4, "ProductCategoryID");
                        //adapter.UpdateCommand.UpdatedRowSource = UpdateRowSource.None;


                        //// Set the batch size.
                        //adapter.UpdateBatchSize = batchSize;

                        //// Execute the update.
                        //adapter.Update(dataTable);




                //    }

                //    finished = finished + 1;
                //    Console.Beep(200, 50);



                //}
                //catch (Exception e)
                //{
                //    Console.WriteLine(e.ToString());
                //}
                //myConnection.Close();
            }
        }
    }



    class WeiboMySQL
    {
        static public Coordiante<Int64, String, String> GetCoordinates()
        {
            string query = "SelectCoordianteToTransform";

            //Create a list to store the result
            var CoordinateList = new Coordiante<Int64, String, String> { };

            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.MSSQL);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Connection = MSQconn;

            try
            {
                //CALL
                MSQconn.Open();
                MySqlDataReader dataReader = MSQcommand.ExecuteReader();
                //Read the data and store them in the list

                while (dataReader.Read())
                {
                    //Use same column names as in MySQL DB!!!
                    CoordinateList.Add((Int64)dataReader["msgID"],
                                   (String)dataReader["geoLAT"],//Center coordinates of hexagon
                                   (String)dataReader["geoLOG"]);//Center coordinates of hexagon

                }//close Data Reader
                dataReader.Close();
                MSQconn.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }
            return CoordinateList;
        }



    }

    class SQLServer
    {
        static public Coordiante<Int64, double, double> GetCoordinates(int idx)
        {

            //Create a list to store the result
            var CoordinateList = new Coordiante<Int64, double, double> { };

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            //READ
            try
            {
                SqlDataReader myReader = null;
                //SqlCommand myCommand = new SqlCommand("SELECT TOP 10000 [idNearByTimeLine],[geoLAT],[geoLOG] FROM [weibo].[dbo].[NBT2] Where [idNearByTimeLine]>" + idx + " AND [location] is NULL;",
                //                                         myConnection);

                SqlCommand myCommand = new SqlCommand("SELECT TOP 1000000 [idNearByTimeLine],[geoLAT],[geoLOG] FROM [weiboDEV].[dbo].[NBT2_trans] WHERE [WGSLatitudeX] IS NULL;",
                                                         myConnection);
                myCommand.CommandTimeout = 600;
                myReader = myCommand.ExecuteReader();
                while (myReader.Read())
                {
                    //Console.Write(myReader["RandomID"].ToString() + "\t");
                    //Console.Write(myReader["TimesHarvested"].ToString() + "\t");
                    //Console.Write(myReader["TotalCollected"].ToString() + "\t");
                    //Console.Write(myReader["TotalInserted"].ToString() + "\t");
                    //Console.Write(myReader["LAT"].ToString() + "\t");
                    //Console.Write(myReader["LON"].ToString() + "\t");
                    //Console.Write(myReader["ratio"].ToString() + "\n");

                    CoordinateList.Add(Convert.ToInt32(myReader["idNearByTimeLine"]),          //Item1
                                    Convert.ToDouble(myReader["geoLAT"]),    //Item2
                                    Convert.ToDouble(myReader["geoLOG"]));   //Item3
                }
                myReader.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            myConnection.Close();
            return CoordinateList;
        }

        static public DataTable GetFull100000NBT2table()
        {
            int starthere = 0;

            string query = "SELECT TOP(10) " +
                                     "[idNearByTimeLine]" +
                                    ",[SeasonID]" +
                                    ",[FieldID]" +
                                    ",[FieldGroupID]" +
                                    ",[createdAT]" +
                                    ",[msgID]" +
                                    ",[msgmid]" +
                                    ",[msgidstr]" +
                                    ",[msgtext]" +
                                    ",[msgin_reply_to_status_id]" +
                                    ",[msgin_reply_to_user_id]" +
                                    ",[msgin_reply_to_screen_name]" +
                                    ",[msgfavorited]" +
                                    ",[msgsource]" +
                                    ",[geoTYPE]" +
                                    ",[geoLAT]" +
                                    ",[geoLOG]" +
                                    ",[distance]" +
                                    ",[userID]" +
                                    ",[userscreen_name]" +
                                    ",[userprovince]" +
                                    ",[usercity]" +
                                    ",[userlocation]" +
                                    ",[userdescription]" +
                                    ",[userfollowers_count]" +
                                    ",[userfriends_count]" +
                                    ",[userstatuses_count]" +
                                    ",[userfavourites_count]" +
                                    ",[usercreated_at]" +
                                    ",[usergeo_enabled]" +
                                    ",[userverified]" +
                                    ",[userbi_followers_count]" +
                                    ",[userlang]" +
                                    ",[userclient_mblogid]" +
                                    ",[nearbytimelinecol]" +
                                    ",[RowADDEDtime]" +
                                    ",[WGSLatitudeX]" +
                                    ",[WGSLongitudeY]" +
                                    ",[location]" +
                                    " FROM [weiboDEV].[dbo].[NBT2_trans];";// Where [idNearByTimeLine] > " + starthere + ";";

            //Create a list to store the result
            var CoordinateList = new Coordiante<Int64, String, String> { };

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            SqlCommand myCommand = new SqlCommand();
            myCommand.CommandText = query;
            //MSQcommand.CommandType = CommandType.StoredProcedure;
            myCommand.Connection = myConnection;
            DataTable table = new DataTable();
            table.Columns.Add("idNearByTimeLine"            , typeof(int));
            table.Columns.Add("SeasonID"                    , typeof(int));
            table.Columns.Add("FieldID"                     , typeof(int));
            table.Columns.Add("FieldGroupID"                , typeof(int));
            table.Columns.Add("createdAT"                   , typeof(DateTime));
            table.Columns.Add("msgID"                       , typeof(Int64));
            table.Columns.Add("msgmid"                      , typeof(Int64));
            table.Columns.Add("msgidstr"                    , typeof(Int64));
            table.Columns.Add("msgtext"                     , typeof(string));
            table.Columns.Add("msgin_reply_to_status_id"    , typeof(Int64));
            table.Columns.Add("msgin_reply_to_user_id"      , typeof(Int64));
            table.Columns.Add("msgin_reply_to_screen_name"  , typeof(string));
            table.Columns.Add("msgfavorited"                , typeof(Int16));
            table.Columns.Add("msgsource"                   , typeof(Boolean));
            table.Columns.Add("geoTYPE"                     , typeof(string));
            table.Columns.Add("geoLAT"                      , typeof(double));
            table.Columns.Add("geoLOG"                      , typeof(double));
            table.Columns.Add("distance"                    , typeof(int));
            table.Columns.Add("userID"                      , typeof(Int64));
            table.Columns.Add("userscreen_name"             , typeof(string));
            table.Columns.Add("userprovince"                , typeof(int));
            table.Columns.Add("usercity"                    , typeof(int));
            table.Columns.Add("userlocation"                , typeof(string));
            table.Columns.Add("userdescription"             , typeof(string));
            table.Columns.Add("userfollowers_count"         , typeof(int));
            table.Columns.Add("userfriends_count"           , typeof(int));
            table.Columns.Add("userstatuses_count"          , typeof(int));
            table.Columns.Add("userfavourites_count"        , typeof(int));
            table.Columns.Add("usercreated_at"              , typeof(DateTime));
            table.Columns.Add("usergeo_enabled"             , typeof(int));
            table.Columns.Add("userverified"                , typeof(int));
            table.Columns.Add("userbi_followers_count"      , typeof(int));
            table.Columns.Add("userlang"                    , typeof(string));
            table.Columns.Add("userclient_mblogid"          , typeof(string));
            table.Columns.Add("nearbytimelinecol"           , typeof(string));
            table.Columns.Add("RowADDEDtime"                , typeof(DateTime));
            table.Columns.Add("WGSLatitudeX"                , typeof(double));
            table.Columns.Add("WGSLongitudeY"               , typeof(double));
            //table.Columns.Add("location"                    , typeof(SqlGeography));
            try
            {
                SqlDataReader dataReader = null;
                
                //DataRow newRow = table.NewRow();
                //newRow["location"] = SqlGeometry.Point(0.0, 0.0, 4326);
                dataReader = myCommand.ExecuteReader();
                while (dataReader.Read())
                {
                    ////Use same column names as in MySQL DB!!!
                    //CoordinateList.Add((Int64)dataReader["msgID"],
                    //               (String)dataReader["geoLAT"],//Center coordinates of hexagon
                    //               (String)dataReader["geoLOG"]);//Center coordinates of hexagon


                    table.Rows.Add(
                        (int)dataReader["idNearByTimeLine"],
                        (int)dataReader["SeasonID"],
                        (int)dataReader["FieldID"],
                        (int)dataReader["FieldGroupID"],
                        (DateTime)dataReader["createdAT"],
                        (Int64)dataReader["msgID"],
                        (Int64)dataReader["msgmid"],
                        (Int64)dataReader["msgidstr"],
                        (string)dataReader["msgtext"],
                        (Int64)dataReader["msgin_reply_to_status_id"],
                        (Int64)dataReader["msgin_reply_to_user_id"],
                        (string)dataReader["msgin_reply_to_screen_name"],
                        (Int16)dataReader["msgfavorited"],
                        //Boolean.Parse(Convert.ToString(dataReader["msgsource"])),
                      (Boolean)false,
                                    Convert.ToString(dataReader["geoTYPE"]),
                        Double.Parse(Convert.ToString(dataReader["geoLAT"])),
                        Double.Parse(Convert.ToString(dataReader["geoLOG"])),
                        int.Parse(Convert.ToString(dataReader["distance"])),
                        Int64.Parse(Convert.ToString(dataReader["userID"])),
                                    Convert.ToString(dataReader["userscreen_name"]),
                        int.Parse(Convert.ToString(dataReader["userprovince"])),
                        int.Parse(Convert.ToString(dataReader["usercity"])),
                                    Convert.ToString(dataReader["userlocation"]),
                                    Convert.ToString(dataReader["userdescription"]),
                        int.Parse(Convert.ToString(dataReader["userfollowers_count"])),
                        int.Parse(Convert.ToString(dataReader["userfriends_count"])),
                        int.Parse(Convert.ToString(dataReader["userstatuses_count"])),
                        int.Parse(Convert.ToString(dataReader["userfavourites_count"])),
                        (DateTime)dataReader["usercreated_at"],
                        int.Parse(Convert.ToString(dataReader["usergeo_enabled"])),
                        int.Parse(Convert.ToString(dataReader["userverified"])),
                        int.Parse(Convert.ToString(dataReader["userbi_followers_count"])),
                        Convert.ToString(dataReader["userlang"]),
                        Convert.ToString(dataReader["userclient_mblogid"]),
                        Convert.ToString(dataReader["nearbytimelinecol"]),
                        (DateTime)dataReader["RowADDEDtime"],
                        Double.Parse(Convert.ToString(dataReader["WGSLatitudeX"])),
                        Double.Parse(Convert.ToString(dataReader["WGSLongitudeY"]))
                        //Convert.ToString(dataReader["location"])
                        //newRow


                        );

                }//close Data Reader
                dataReader.Close();
                myConnection.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }
            return table;
        }

        static public DataTable READDataTableFromSQLServer(int id, int top) {

            string query = "SELECT TOP("+ top.ToString() +") " +
                                     "[idNearByTimeLine]" +
                                    ",[SeasonID]" +
                                    ",[FieldID]" +
                                    ",[FieldGroupID]" +
                                    ",[createdAT]" +
                                    ",[msgID]" +
                                    ",[msgmid]" +
                                    ",[msgidstr]" +
                                    ",[msgtext]" +
                                    ",[msgin_reply_to_status_id]" +
                                    ",[msgin_reply_to_user_id]" +
                                    ",[msgin_reply_to_screen_name]" +
                                    ",[msgfavorited]" +
                                    ",[msgsource]" +
                                    ",[geoTYPE]" +
                                    ",[geoLAT]" +
                                    ",[geoLOG]" +
                                    ",[distance]" +
                                    ",[userID]" +
                                    ",[userscreen_name]" +
                                    ",[userprovince]" +
                                    ",[usercity]" +
                                    ",[userlocation]" +
                                    ",[userdescription]" +
                                    ",[userfollowers_count]" +
                                    ",[userfriends_count]" +
                                    ",[userstatuses_count]" +
                                    ",[userfavourites_count]" +
                                    ",[usercreated_at]" +
                                    ",[usergeo_enabled]" +
                                    ",[userverified]" +
                                    ",[userbi_followers_count]" +
                                    ",[userlang]" +
                                    ",[userclient_mblogid]" +
                                    ",[nearbytimelinecol]" +
                                    ",[RowADDEDtime]" +
                                    ",[WGSLatitudeX]" +
                                    ",[WGSLongitudeY]" +
                                    ",[location]" +
                                    " FROM [weibotest2].[dbo].[NBT2] Where [idNearByTimeLine] > " + id.ToString() + ";";


            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }


            var table = new DataTable();
            using (var da = new SqlDataAdapter(query, myConnection))
            {
                da.Fill(table);
            }

            myConnection.Close();

            return table;
        
        
        }

        static public bool WRITEDataTableToSQLServer(string tableName, DataTable dataTable, string database)
        {

            bool isSuccuss;
            try
            {
                SqlConnection SqlConnectionObj = SQLServer.GetSQLServerConnection(database);
                SqlBulkCopy bulkCopy = new SqlBulkCopy(SqlConnectionObj, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, null);
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.WriteToServer(dataTable);
                isSuccuss = true;
            }
            catch (Exception ex)
            {
                isSuccuss = false;
            }
            return isSuccuss;

        }

        static public Tuple<int, int> NearbyTimelineToSQLpTrans(dynamic s, string sid, string fid, int fgid)
        {

            int counts = 0;
            int written = 0;

            //InsertCommand
            const string insertCommand = "INSERT INTO [weibo].[dbo].[NBT2] (" +
                                         "SeasonID," +
                                         "FieldID," +
                                         "FieldGroupID," +
                                         "createdAT," +
                                         "msgID," +
                                         "msgmid," +
                                         "geoTYPE," +
                                         "geoLAT," +
                                         "geoLOG," +
                                         "userID," +
                                         "distance," +
                //InnoDB// "Point," +
                                         "msgidstr," +
                                         "msgtext," +
                                         "msgin_reply_to_status_id," +
                                         "msgin_reply_to_user_id," +
                                         "msgin_reply_to_screen_name," +
                                         "msgfavorited," +
                                         "userscreen_name," +
                                         "userprovince," +
                                         "usercity," +
                                         "userlocation," +
                                         "userfollowers_count," +
                                         "userfriends_count," +
                                         "userstatuses_count," +
                                         "userfavourites_count," +
                                         "usercreated_at," +
                                         "usergeo_enabled," +
                                         "userverified," +
                                         "userbi_followers_count," +
                                         "userlang," +
                                         "userclient_mblogid," +
                                         "WGSLatitudeX," +
                                         "WGSLongitudeY," +
                                         "location" +
                //"userdescription" +
                                         ")" +

                                         " values " +
                                         "(" +
                                         "@SID," +
                                         "@FID," +
                                         "@FGID," +
                                         "@createdAT," +
                                         "@msgid," +
                                         "@msgmid," +
                                         "@geoTYPE," +
                                         "@geoLAT," +
                                         "@geoLOG," +
                                         "@userID," +
                                         "@distance," +
                //InnoDB// "GeomFromText(@Point)," +
                                         "@msgidstr," +
                                         "@msgtext," +
                                         "@msgin_reply_to_status_id," +
                                         "@msgin_reply_to_user_id," +
                                         "@msgin_reply_to_screen_name," +
                                         "@msgfavorited," +
                                         "@userscreen_name," +
                                         "@userprovince," +
                                         "@usercity," +
                                         "@userlocation," +
                                         "@userfollowers_count," +
                                         "@userfriends_count," +
                                         "@userstatuses_count," +
                                         "@userfavourites_count," +
                                         "@usercreated_at," +
                                         "1," +
                                         "@userverified," +
                                         "@userbi_followers_count," +
                                         "@userlang," +
                                         "@userclient_mblogid," +
                                         "@WGSLatitudeX," +
                                         "@WGSLongitudeY," +
                                         "geography::STGeomFromText(@location,4326)" +
                //"@userdescription" +
                                         ")";
            try
            {
                using (var tsc = new TransactionScope()) //Not sure if it is implemented properly!
                {
                    //Console.WriteLine("In TransacrtionScope");
                    using (var conn1 = new SqlConnection(Properties.Settings.Default.MSSQL))
                    {
                        conn1.Open();
                        const string PointMask = "POINT ({0} {1})";


                        //Console.Write(" conn1 Open ");
                        //Console.WriteLine(s);
                        foreach (var result in s["statuses"])
                        {
                            SqlCommand cmd = new SqlCommand(insertCommand, conn1);
                            counts = counts + 1;
                            //Console.WriteLine("Status nr. " + counts.ToString());
                            var geoEnabled = (string)result["user"]["geo_enabled"];
                            if (String.Equals(geoEnabled, "true", StringComparison.InvariantCultureIgnoreCase))
                            {
                                //cmd.Parameters.Add
                                cmd.Parameters.Add("@SID", System.Data.SqlDbType.Int).Value = sid;
                                cmd.Parameters.Add("@FID", System.Data.SqlDbType.Int).Value = fid;
                                cmd.Parameters.Add("@FGID", System.Data.SqlDbType.Int).Value = fgid;

                                var createdAt = (string)result["created_at"];
                                DateTime myDate = DateTime.ParseExact(createdAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                                    System.Globalization.CultureInfo.InvariantCulture);
                                //myDate.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                                cmd.Parameters.Add("@createdAT", System.Data.SqlDbType.DateTime).Value =
                                    myDate.ToUniversalTime();


                                cmd.Parameters.Add("@msgid", System.Data.SqlDbType.BigInt).Value =
                                    long.Parse(Convert.ToString(result["id"]));
                                //Console.WriteLine(result["id"]);


                                cmd.Parameters.Add("@msgmid", System.Data.SqlDbType.BigInt).Value =
                                    long.Parse(Convert.ToString(result["mid"]));


                                cmd.Parameters.Add("@geoTYPE", System.Data.SqlDbType.VarChar).Value = Convert.ToString(result["geo"]["type"]);
                                cmd.Parameters.Add("@geoLAT", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(result["geo"]["coordinates"][0]));
                                cmd.Parameters.Add("@geoLOG", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(result["geo"]["coordinates"][1]));

                                //cmd.Parameters.AddWithValue("@usergeo_enabled", 1);

                                cmd.Parameters.Add("@userID", System.Data.SqlDbType.BigInt).Value = long.Parse(Convert.ToString(result["user"]["id"]));
                                cmd.Parameters.Add("@distance", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["distance"]));
                                cmd.Parameters.Add("@msgidstr", System.Data.SqlDbType.BigInt).Value = long.Parse(Convert.ToString(result["idstr"]));
                                cmd.Parameters.Add("@msgtext", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["text"]);

                                string inReplyToStatusId = Convert.ToString(result["in_reply_to_status_id"]);
                                if (inReplyToStatusId.Any())
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_status_id", System.Data.SqlDbType.BigInt).Value = long.Parse(inReplyToStatusId);
                                }
                                else
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_status_id", System.Data.SqlDbType.BigInt).Value =
                                        DBNull.Value;
                                }

                                string inReplyToUserId = Convert.ToString(result["in_reply_to_user_id"]);

                                if (inReplyToUserId != null && inReplyToUserId.Any())
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_user_id", System.Data.SqlDbType.BigInt).Value = long.Parse(inReplyToUserId);
                                }
                                else
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_user_id", System.Data.SqlDbType.BigInt).Value =
                                        DBNull.Value;
                                }

                                cmd.Parameters.Add("@msgin_reply_to_screen_name", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["in_reply_to_screen_name"]);

                                string msgfavorited = Convert.ToString(result["favorited"]);
                                cmd.Parameters.Add("@msgfavorited", System.Data.SqlDbType.Int).Value = msgfavorited.ToLower() == "true" ? 1 : 0;


                                cmd.Parameters.Add("@userscreen_name", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["user"]["screen_name"]);

                                cmd.Parameters.Add("@userprovince", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["province"]));

                                cmd.Parameters.Add("@usercity", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["city"]));
                                cmd.Parameters.Add("@userlocation", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["user"]["location"]);

                                cmd.Parameters.Add("@userfollowers_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["followers_count"]));
                                cmd.Parameters.Add("@userfriends_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["friends_count"]));
                                cmd.Parameters.Add("@userstatuses_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["statuses_count"]));
                                cmd.Parameters.Add("@userfavourites_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["favourites_count"]));


                                string usercreatedAt = result["user"]["created_at"];
                                myDate = DateTime.ParseExact(usercreatedAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                                    System.Globalization.CultureInfo.InvariantCulture);

                                cmd.Parameters.Add("@usercreated_at", System.Data.SqlDbType.DateTime).Value =
                                    myDate.ToUniversalTime();

                                string verified = Convert.ToString(result["user"]["verified"]);
                                cmd.Parameters.Add("@userverified", System.Data.SqlDbType.Int).Value = verified.ToLower() == "true" ? 1 : 0;

                                string userbiFollowersCount = Convert.ToString(result["user"]["bi_followers_count"]);
                                if (userbiFollowersCount.Any())
                                {
                                    cmd.Parameters.Add("@userbi_followers_count", System.Data.SqlDbType.Int).Value =
                                        int.Parse(userbiFollowersCount);
                                }
                                else
                                {
                                    cmd.Parameters.Add("@userbi_followers_count", System.Data.SqlDbType.Int).Value = DBNull.Value;
                                }

                                cmd.Parameters.Add("@userlang", System.Data.SqlDbType.VarChar).Value = Convert.ToString(result["user"]["lang"]);

                                string mblogid = Convert.ToString(result["user"]["client_mblogid"]);
                                if (mblogid != null && mblogid.Any())
                                {
                                    cmd.Parameters.Add("@userclient_mblogid", System.Data.SqlDbType.NVarChar).Value = mblogid;
                                }
                                else
                                {
                                    cmd.Parameters.Add("@userclient_mblogid", System.Data.SqlDbType.NVarChar).Value =
                                        DBNull.Value;
                                }
                                ////cmd.Parameters.AddWithValue("@userdescription", result["user"]["description"]);

                                EvilTransform.Transform transformer = new EvilTransform.Transform();
                                EvilTransform.PointLatLng WGSpoint = new EvilTransform.PointLatLng();
                                double badLat = double.Parse(Convert.ToString(result["geo"]["coordinates"][0]));
                                double badLog = double.Parse(Convert.ToString(result["geo"]["coordinates"][1]));
                                WGSpoint = transformer.GCJ2WGSExact(badLat, badLog);
                                //int epsg = 4326;
                                //var point = Microsoft.SqlServer.Types.SqlGeography.Point(WGSpoint.Lat, WGSpoint.Lng, epsg); //srid not EPSG here

                                cmd.Parameters.Add("@WGSLatitudeX", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(WGSpoint.Lat));
                                cmd.Parameters.Add("@WGSLongitudeY", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(WGSpoint.Lng));

                                string location = string.Format(PointMask, WGSpoint.Lng, WGSpoint.Lat);
                                //Console.WriteLine(location);
                                cmd.Parameters.Add("@location", System.Data.SqlDbType.NVarChar).Value = location;



                            }//If geoenabled
                            //Console.Write(counts.ToString());
                            try
                            {
                                //SQL throws a VIOLATION not the number of rows affected (not so nice)
                                written += cmd.ExecuteNonQuery();

                            }
                            catch (Exception ex)
                            {
                                //Console.WriteLine(ex);
                            }

                        }//For each Loop
                        if (written >= 1 && written <= 50)
                        {
                            //good
                        }
                        else
                        {
                            written = 0;
                        }
                        //Console.Write("written: " + written.ToString());
                        //cmd.Parameters.Clear();
                        //conn1.Close();
                    }//SQL connection
                    tsc.Complete();

                }//TransactionScope
            }
            catch (TransactionAbortedException ex)
            {
                Console.WriteLine("TransactionAbortedException Message: {0}", ex.Message);
            }
            catch (ApplicationException ex)
            {
                Console.WriteLine("ApplicationException Message: {0}", ex.Message);
            }
            var res = new Tuple<int, int>(counts, written);
            return res;
        }

        static public SqlConnection GetSQLServerConnection(string db) {
           
            
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            //CONNECT
            if (db=="weiboDEV"){
                myConnection = new SqlConnection(Properties.Settings.Default.MSSQLweiboDEV);

            }
            //Console.WriteLine(myConnection.ToString());   
       

            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
       return myConnection;
        }
        

    }


    //Tuple from MySQL DB
    public class Coordiante<T1, T2, T3> : List<Tuple<T1, T2, T3>>
    {
        public void Add(T1 item, T2 item2, T3 item3)
        {
            Add(new Tuple<T1, T2, T3>(item, item2, item3));
        }
    }

}
namespace EvilTransform
{
    public struct PointLatLng
    {
        public double Lat;
        public double Lng;

        public PointLatLng(double lat, double lng)
        {
            this.Lat = lat;
            this.Lng = lng;
        }
    }
    class Transform
    {
        bool OutOfChina(double lat, double lng)
        {
            if ((lng < 72.004) || (lng > 137.8347))
            {
                return true;
            }
            if ((lat < 0.8293) || (lat > 55.8271))
            {
                return true;
            }
            return false;
        }

        double TransformLat(double x, double y)
        {
            double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.Sqrt(Math.Abs(x));
            ret += (20.0 * Math.Sin(6.0 * x * Math.PI) + 20.0 * Math.Sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
            ret += (20.0 * Math.Sin(y * Math.PI) + 40.0 * Math.Sin(y / 3.0 * Math.PI)) * 2.0 / 3.0;
            ret += (160.0 * Math.Sin(y / 12.0 * Math.PI) + 320 * Math.Sin(y * Math.PI / 30.0)) * 2.0 / 3.0;
            return ret;
        }

        double TransformLon(double x, double y)
        {
            double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.Sqrt(Math.Abs(x));
            ret += (20.0 * Math.Sin(6.0 * x * Math.PI) + 20.0 * Math.Sin(2.0 * x * Math.PI)) * 2.0 / 3.0;
            ret += (20.0 * Math.Sin(x * Math.PI) + 40.0 * Math.Sin(x / 3.0 * Math.PI)) * 2.0 / 3.0;
            ret += (150 * Math.Sin(x / 12.0 * Math.PI) + 300.0 * Math.Sin(x / 30.0 * Math.PI)) * 2.0 / 3.0;
            return ret;
        }

        PointLatLng Delta(double lat, double lng)
        {
            PointLatLng ret = new PointLatLng();
            double a = 6378245.0;
            double ee = 0.00669342162296594323;
            double dLat = TransformLat(lng - 105.0, lat - 35.0);
            double dLng = TransformLon(lng - 105.0, lat - 35.0);
            double radLat = lat / 180.0 * Math.PI;
            double magic = Math.Sin(radLat);
            magic = 1 - ee * magic * magic;
            double sqrtMagic = Math.Sqrt(magic);
            dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
            dLng = (dLng * 180.0) / (a / sqrtMagic * Math.Cos(radLat) * Math.PI);
            ret.Lat = dLat;
            ret.Lng = dLng;
            return ret;
        }

        public PointLatLng WGS2GCJ(double wgsLat, double wgsLng)
        {
            if (OutOfChina(wgsLat, wgsLng))
            {
                return new PointLatLng(wgsLat, wgsLng);
            }
            PointLatLng d = Delta(wgsLat, wgsLng);
            return new PointLatLng(wgsLat + d.Lat, wgsLng + d.Lng);
        }

        public PointLatLng GCJ2WGS(double gcjLat, double gcjLng)
        {
            if (OutOfChina(gcjLat, gcjLng))
            {
                return new PointLatLng(gcjLat, gcjLng);
            }
            PointLatLng d = Delta(gcjLat, gcjLng);
            return new PointLatLng(gcjLat - d.Lat, gcjLng - d.Lng);
        }

        public PointLatLng GCJ2WGSExact(double gcjLat, double gcjLng)
        {
            double initDelta = 0.01;
            double threshold = 0.000001;
            double dLat = initDelta;
            double dLng = initDelta;
            double mLat = gcjLat - dLat;
            double mLng = gcjLng - dLng;
            double pLat = gcjLat + dLat;
            double pLng = gcjLng + dLng;
            double wgsLat = 0;
            double wgsLng = 0;

            for (int i = 0; i < 30; i++)
            {
                wgsLat = (mLat + pLat) / 2;
                wgsLng = (mLng + pLng) / 2;
                PointLatLng tmp = WGS2GCJ(wgsLat, wgsLng);
                dLat = tmp.Lat - gcjLat;
                dLng = tmp.Lng - gcjLng;
                if ((Math.Abs(dLat) < threshold) && (Math.Abs(dLng) < threshold))
                {
                    return new PointLatLng(wgsLat, wgsLng);
                }
                if (dLat > 0)
                {
                    pLat = wgsLat;
                }
                else
                {
                    mLat = wgsLat;
                }
                if (dLng > 0)
                {
                    pLng = wgsLng;
                }
                else
                {
                    mLng = wgsLng;
                }
            }
            return new PointLatLng(wgsLat, wgsLng);
        }

        public double Distance(double latA, double lngA, double latB, double lngB)
        {
            double earthR = 6371000;
            double x = Math.Cos(latA * Math.PI / 180) * Math.Cos(latB * Math.PI / 180) * Math.Cos((lngA - lngB) * Math.PI / 180);
            double y = Math.Sin(latA * Math.PI / 180) * Math.Sin(latB * Math.PI / 180);
            double s = x + y;
            if (s > 1)
                s = 1;
            if (s < -1)
                s = -1;
            double alpha = Math.Acos(s);
            var distance = alpha * earthR;
            return distance;
        }
    }
}




