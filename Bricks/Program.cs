using System;
using System.Collections.Generic;
using System.Configuration;
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
            Console.WriteLine("Программа завершила выполнение");
        }

        private static void GetValue()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["SQLConnection"].ConnectionString;
            SqlConnection con = new SqlConnection(connectionString);
            con.Open();

            string maxIdQuery = "SELECT Max([id]) FROM[SecondarySalesRussia_TQ].[dbo].[Personnel_District]";

            SqlCommand cmdMax = new SqlCommand(maxIdQuery, con);
            int maxIdProfUnicId = (int)cmdMax.ExecuteScalar();

            string firstValueQuery = "SELECT Top(1) id FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District] where DateIn <= GETDATE() and DateOut >= GETDATE()";
            SqlCommand cmdFirstValue = new SqlCommand(firstValueQuery, con);
            int firstVal = (int)cmdFirstValue.ExecuteScalar();

            List<int> ProfUnicIdArr = new List<int>();  // Уникальные ProfUnicID, которые соотвествуют дате

            for (int i = firstVal; i <= maxIdProfUnicId; i++)
            {
                string arrQuery = $@"SELECT Distinct [ProfUniqID]
                                  FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]
                                  where DateIn <= GETDATE() and DateOut >= GETDATE() and id = {i}";
                SqlCommand cmdArr = new SqlCommand(arrQuery, con);
                int ProfUnicIdToArr = 0;


                try
                {
                    ProfUnicIdToArr = (int)cmdArr.ExecuteScalar();
                    if (!ProfUnicIdArr.Contains(ProfUnicIdToArr))
                    {
                        ProfUnicIdArr.Add(ProfUnicIdToArr); // получили актуальные должности
                    }
                }
                catch (Exception) { } // there is not such id 

            }
            ProfUnicIdArr.Add(14573);
            //ProfUnicIdArr.Add(14578);

            string pos = String.Empty;
            List<int> DivisionIdArr = new List<int>();
            // Определим подразделения divisionID чтобы понять входить ли эта должность в подразделение 3213


            for (int j = 0; j < ProfUnicIdArr.Count(); j++)
            {
                int DiviToArr = 0;
                int parentDiToArr = 0;
                int ProfId = ProfUnicIdArr.ElementAt(j);
                List<int> DivisionIdAndParent = new List<int>();
                string divisionIdQuery = $@"SELECT[DivisionID]
                                                  FROM[Applications].[dbo].[Staff_Profession]
                                                  where[ProfessionID] = {ProfId}";
                SqlCommand cmdArrDiv = new SqlCommand(divisionIdQuery, con);
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

                    }
                    catch (Exception) { }
                    DivisionIdAndParent.Add(parentDiToArr);
                    DiviToArr = parentDiToArr;
                }
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
                        for (int i = 1; i <= maxIdProfUnicId; i++) // maxIdProfUnicId
                        {
                            IdFromPersDist = i;
                            string subquery = $@"SELECT [SecondarySalesDistrictID]
                                      FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]
                                      inner join [SecondarySalesRussia_TQ].[dbo].[Ref_SecondarySalesDistrict]
                                      ON [Personnel_District].SecondarySalesDistrictID = [Ref_SecondarySalesDistrict].id
                                      where [ProfUniqID] = {ProfId} and DateIn <= GETDATE() and DateOut >= GETDATE() and [Personnel_District].id = {i}
                                      and [LevelValue] < 4";
                            SqlCommand cmdsubquery = new SqlCommand(subquery, con);

                            try
                            {
                                secSalDistrictId = (int)cmdsubquery.ExecuteScalar();
                                if (!secondarySalesDistrictId.Contains(secSalDistrictId))
                                {
                                    secondarySalesDistrictId.Add(secSalDistrictId); // Получаем значения территории у данной должности где levelValue < 4 их может быть несколько
                                }
                                // имея эти знаяения найдём levelValue
                            }
                            catch (Exception) { }
                        }

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
                                    string maxIdSecQuery = $@"SELECT MAX([id])
                                            FROM[SecondarySalesRussia].[dbo].[Ref_SecondarySalesDistrict]";
                                    SqlCommand cmdmaxIdSec = new SqlCommand(maxIdSecQuery, con);
                                    int maxIdSecSal = (int)cmdmaxIdSec.ExecuteScalar();

                                    List<int> childList = new List<int>();
                                    int maxIdPers = 0;
                                    for (int k = 1; k < maxIdSecSal; k++) // maxIdSecSal
                                    {
                                        string childIdQuery = $@"SELECT [id]
                                                    FROM [SecondarySalesRussia_TQ].[dbo].[Ref_SecondarySalesDistrict]
                                                    where ParentID = {secondarySalesDistrictId.ElementAt(i)}
                                                    and IsActive = 1 and CodeIMS is not null
	                                                and id = {k}";
                                        SqlCommand cmdChildId = new SqlCommand(childIdQuery, con);

                                        // вычислим максимальное значение personnel_District
                                        string maxIdPersDis = $@"SELECT max([id])
                                                          FROM [SecondarySalesRussia_TQ].[dbo].[Personnel_District]";
                                        SqlCommand cmdmaxIdPersDis = new SqlCommand(maxIdPersDis, con);
                                        maxIdPers = (int)cmdmaxIdPersDis.ExecuteScalar();
                                        
                                        try
                                        {
                                            int ChildId = (int)cmdChildId.ExecuteScalar();
                                            childList.Add(ChildId);
                                        }
                                        catch (Exception) { }
                                    }
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