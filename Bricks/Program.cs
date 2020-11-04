using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bricks
{
    class Program
    {
        static void Main(string[] args)
        {
            GetValue();
            Console.WriteLine("Программа успешно завершила выполнение");
        }

        private static void GetValue()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["SQLConnection"].ConnectionString;
            SqlConnection con = new SqlConnection(connectionString);
            con.Open();

            //
            // Это я потом удалю
            string maxIdQuery = "SELECT Max([id]) FROM[SecondarySalesRussia_TQ].[dbo].[Personnel_District]";

            SqlCommand cmdMax = new SqlCommand(maxIdQuery, con);
            int maxIdProfUnicId = (int)cmdMax.ExecuteScalar();
            //





            string arrProfUnicIdQuery = $@"SELECT Distinct [ProfUniqID]
                                  FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]
                                  where DateIn <= GETDATE() and DateOut >= GETDATE()";
            SqlCommand cmdArr = new SqlCommand(arrProfUnicIdQuery, con);

            SqlDataAdapter adapter = new SqlDataAdapter(arrProfUnicIdQuery, con);

            DataSet ds = new DataSet("tables");

            adapter.Fill(ds, "Personnel_District");

            DataTable dataTable;
            dataTable = ds.Tables["Personnel_District"];

            List<int> ProfUnicIdArr = new List<int>();  // Уникальные ProfUnicID, которые соотвествуют дате

            foreach (DataRow drCurrent in dataTable.Rows)
            {
                ProfUnicIdArr.Add(Convert.ToInt32(drCurrent["ProfUniqID"]));
            }

            string pos = String.Empty;
            List<int> DivisionIdArr = new List<int>();
            //Определим подразделения divisionID чтобы понять входить ли эта должность в подразделение 3213


            for (int j = 1088; j < ProfUnicIdArr.Count(); j++)
            {
                int DiviToArr = 0;
                int parentDiToArr = 0;
                int ProfId = ProfUnicIdArr.ElementAt(j);
                List<int> DivisionIdAndParent = new List<int>();
                string divisionIdQuery = $@"SELECT[DivisionID]
                                                  FROM[Applications].[dbo].[Staff_Profession]
                                                  where[ProfessionID] = {ProfId}";
                SqlCommand cmdArrDiv = new SqlCommand(divisionIdQuery, con);

                try
                {
                    DiviToArr = (int)cmdArrDiv.ExecuteScalar();
                    DivisionIdAndParent.Add(DiviToArr);

                    while (parentDiToArr != 1300)
                    {
                        // нашли divisionId нужно найти все его parentDivisionId
                        string parentDiv = $@"SELECT Distinct [ParentDivisionID]
                                        FROM [Applications].[dbo].[Staff_Division]
                                        where [DivisionID] = {DiviToArr}
                                        and DateIn <= GETDATE() and DateOut >= GETDATE()";
                        SqlCommand cmdparentDiv = new SqlCommand(parentDiv, con);
                        try
                        {
                            parentDiToArr = (int)cmdparentDiv.ExecuteScalar(); // нашли первый parentId ищем дальше

                            DivisionIdAndParent.Add(parentDiToArr);
                            DiviToArr = parentDiToArr;
                            if (parentDiToArr == 1001)
                            {
                                break;
                            }
                        }
                        catch (Exception) { break; }

                    }
                }
                catch (Exception) { }




                int secSalDistrictId = 0;
                // Содержит ли DivisionIdAndParent 3213
                if (DivisionIdAndParent.Contains(3213))
                {
                    // Значит должность в подразделении CHC, потенциально нужно апдейтить
                    // нужно проверить что этот сотрудник занимает должность мед представителя или регионального менеджера
                    string positionQuery = $@"SELECT [JobTitle]
                              FROM [Applications].[dbo].[Staff_Profession]
                              inner join [Applications].[dbo].[Staff_JobTitle]
                              ON [Staff_Profession].JobTitleID = [Staff_JobTitle].id
                              where [ProfessionID] = {ProfId} ";
                    SqlCommand cmdPositionDiv = new SqlCommand(positionQuery, con);
                    pos = (string)cmdPositionDiv.ExecuteScalar();
                    if (pos == "Медицинский представитель" || pos == "Старший медицинский представитель" || pos == "Региональный менеджер" || pos == "Старший региональный менеджер") // 13 30
                    {   // сделать подзапрос чтобы получить [SecondarySalesDistrictID]
                        List<int> secondarySalesDistrictId = new List<int>();
                        int IdFromPersDist = 0;

                        string subquery = $@"SELECT [SecondarySalesDistrictID]
                                      FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]
                                      inner join [SecondarySalesRussia_TQ].[dbo].[Ref_SecondarySalesDistrict]
                                      ON [Personnel_District].SecondarySalesDistrictID = [Ref_SecondarySalesDistrict].id
                                      where [ProfUniqID] = {ProfId} and DateIn <= GETDATE() and DateOut >= GETDATE()
                                      and [LevelValue] < 4";

                        SqlDataAdapter adapter2 = new SqlDataAdapter(subquery, con);

                        DataSet ds2 = new DataSet();

                        adapter2.Fill(ds2, "SecondarySalesDistrictID");

                        DataTable dataTable2;
                        dataTable2 = ds2.Tables["SecondarySalesDistrictID"];

                        // пройдёмся по дататэйбл
                        if (dataTable2 != null)
                        {
                            foreach (DataRow drCurrent2 in dataTable2.Rows)
                            {

                                int secSalDistrictIda = Convert.ToInt32(drCurrent2["SecondarySalesDistrictID"]);

                                if (!secondarySalesDistrictId.Contains(secSalDistrictIda))
                                {
                                    secondarySalesDistrictId.Add(secSalDistrictIda);
                                    //
                                    //
                                    // тут ок
                                }
                            }
                        }
                        //
                        //
                        //

                        for (int i = 0; i < secondarySalesDistrictId.Count; i++)
                        {
                            // имея эти знаяения найдём levelValue

                            string levelValQuery = $@"SELECT [LevelValue]
                                        FROM [SecondarySalesRussia_TQ].[dbo].[Ref_SecondarySalesDistrict]
                                        where id = {secondarySalesDistrictId.ElementAt(i)}";
                            SqlCommand cmdLevelquery = new SqlCommand(levelValQuery, con);

                            //Чтобы найти IdPersennelDistrc
                            string IdFromPersDistQuery = $@"SELECT [id]
                                          FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]
                                            where [SecondarySalesDistrictID] = {secondarySalesDistrictId.ElementAt(i)}
	                                        and DateIn <= GETDATE() and DateOut >= GETDATE()
	                                        and [ProfUniqID] = {ProfId}";
                            SqlCommand cmdPersDist = new SqlCommand(IdFromPersDistQuery, con);
                            int persDist = (int)cmdPersDist.ExecuteScalar(); // Здесь получаем id строки которую обновим
                            try
                            {
                                short levelV = (short)cmdLevelquery.ExecuteScalar();
                                if (levelV == 3)
                                {
                                    // Деактивируем родительскую запись с levelValue = 3
                                    string DeactivateQuery = $@"Update [SecondarySalesRussia_TQ].[dbo].[Personnel_District] set
                                                                DateOut='2020-10-20', ChangeDate=GETDATE() where id = {persDist}";
                                    SqlCommand cmdDeactivate = new SqlCommand(DeactivateQuery, con);
                                    try
                                    {
                                        cmdDeactivate.ExecuteNonQuery();
                                    }
                                    catch (Exception) { }

                                    // Получаем дочерние записи от secondarySalesDistrictId.ElementAt(i) то есть от 477
                                    // Для этого нужно взять максимальное число из таблицы Ref_SecondarySalesDistrict

                                    string childQuery = $@"SELECT [id]
                                      FROM [SecondarySalesRussia_TQ].[dbo].[Ref_SecondarySalesDistrict]
                                      where [ParentID] = {secondarySalesDistrictId.ElementAt(i)}";

                                    SqlDataAdapter adapter3 = new SqlDataAdapter(childQuery, con);
                                    DataSet ds3 = new DataSet("tables");
                                    adapter3.Fill(ds3, "id");
                                    DataTable dataTable3;
                                    dataTable3 = ds3.Tables["id"];

                                    List<int> childList = new List<int>();


                                    foreach (DataRow drCurrent3 in dataTable3.Rows)
                                    {
                                        childList.Add(Convert.ToInt32(drCurrent3["id"]));
                                    }


                                    // вычислим максимальное значение personnel_District
                                    string maxIdPersDis = $@"SELECT max([id])
                                                          FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]";
                                    SqlCommand cmdmaxIdPersDis = new SqlCommand(maxIdPersDis, con);

                                    int maxIdPers = 0;
                                    maxIdPers = (int)cmdmaxIdPersDis.ExecuteScalar();
                                    
                                    try
                                    {
                                        int autoIncrement = 1;
                                        for (int y = 0; y < childList.Count; y++)
                                        {
                                            string insertQuery = $@"insert into [SecondarySalesRussia_TQ].[dbo].[Personnel_District]
	                                                    ([id]
                                                          ,[ProfUniqID]
                                                          ,[SecondarySalesDistrictID]
                                                          ,[DateIn]
                                                          ,[DateOut]
                                                          ,[SecondarySalesDistrictTypeID]
                                                          ,[PersAuthor]
                                                          ,[DateAdd]
                                                          ,[ChangeDate]) 
	                                                      Values
	                                                      ({maxIdPers + autoIncrement}, {ProfId}, {childList.ElementAt(y)}, '2020-10-21', '2100-01-01', 1, 9954, GETDATE(), GETDATE())";
                                            SqlCommand cmdInsert = new SqlCommand(insertQuery, con);
                                            try
                                            {
                                                cmdInsert.ExecuteNonQuery();
                                            }
                                            catch (Exception) { }
                                            autoIncrement++;
                                        }
                                    }
                                    catch (Exception) { }
                                }
                            }
                            catch (Exception) { }
                        }
                    }
                }
            }
        }
    }
}
          

