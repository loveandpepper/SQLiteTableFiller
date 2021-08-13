using System;
using System.Collections.Generic;
using System.Data.SQLite;

namespace SQLiteTableFiller
{
    class Program
    {
        static void Main(string[] args)
        {
            var linesCount = 240000000;
            var sw = new System.Diagnostics.Stopwatch();

            var connection = CreateConnection("Cleverence.Warehouse.Table_БутылкиЕГАИС.sqlite", "Synchronous=Off", "journal mode=Off");
            connection.Open();

            using (var command = connection.CreateCommand())
            {
                var transaction = connection.BeginTransaction();
                command.Transaction = transaction;

                sw.Start();
                var i = 0;
                try
                {
                    command.CommandText = $"INSERT INTO БутылкиЕГАИС(`__UID__`, `АлкоПДФ`, `Коробка`, `Палета`, `Количество`, `АлкоКод`, `АлкоНаим`, `ИдДокумента`, `КодНоменклатуры`, `СкладСерия`, `ФА`, `ФБ`)" +
                        $" VALUES ($uid, $pdf, $box, $pal, $qty, $acode, $name, $doc, $prodId, $serial, $fa, $fb)";

                    var uid = command.CreateParameter();
                    uid.ParameterName = "$uid";
                    var pdf = command.CreateParameter();
                    pdf.ParameterName = "$pdf";
                    var box = command.CreateParameter();
                    box.ParameterName = "$box";
                    var pal = command.CreateParameter();
                    pal.ParameterName = "$pal";
                    
                    command.Parameters.Add(uid);
                    command.Parameters.Add(pdf);
                    command.Parameters.Add(box);
                    command.Parameters.Add(pal);
                    //command.Parameters.AddRange(new object[] { uid, pdf, box, pal }); - я бы сделал так, просто чтобы повыебываться, что тоже что-то могу

                    command.Parameters.AddWithValue("$qty", 1);
                    command.Parameters.AddWithValue("$acode", "0015545000002458847");
                    command.Parameters.AddWithValue("$name", "ЧИСТЫЙ СОСТАВ ВОДКАЯ 40% 0,25Л");
                    command.Parameters.AddWithValue("$doc", Guid.NewGuid().ToString());
                    command.Parameters.AddWithValue("$prodId", "0b4389ad-c7d9-46f4-809f-e2f1d9920b1a");
                    command.Parameters.AddWithValue("$serial", "СКУ-1");
                    command.Parameters.AddWithValue("$fa", "ФА");
                    command.Parameters.AddWithValue("$fb", "ФБ");

                    foreach (var line in GenerateLines())
                    {
                        uid.Value = line.Uid;
                        pdf.Value = line.AlcoPDF;
                        box.Value = line.BoxBarcode;
                        pal.Value = line.PalletBarcode;
                        command.ExecuteNonQuery();

                        i++;
                        if (i % 10000 == 0)
                        {
                            transaction.Commit();
                            Console.WriteLine($"Cгенерировано {i} строк. Потрачено: {sw.Elapsed}");
                            transaction = connection.BeginTransaction();
                            command.Transaction = transaction;
                        }

                        if (i >= linesCount)
                            break;
                    }

                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw new Exception("Ошибка, изменения не записаны! " + ex.Message);
                }
                finally
                {
                    transaction.Commit();
                    sw.Stop();
                    Console.WriteLine($"Cгенерировано {i} строк. Потрачено: {sw.Elapsed}");
                }
            }
            using (var idxCommand = connection.CreateCommand())
            {
                Console.Write($"Построение индексов... ");
                var transaction = connection.BeginTransaction();
                idxCommand.Transaction = transaction;

                sw.Reset();
                sw.Start();
                try
                {
                    idxCommand.CommandText = "CREATE INDEX \"SEARCH_IDX_БутылкиЕГАИС0\" ON \"БутылкиЕГАИС\" (\"АлкоПДФ\"); ";
                    idxCommand.ExecuteNonQuery();

                    idxCommand.CommandText = "CREATE INDEX \"SEARCH_IDX_БутылкиЕГАИС1\" ON \"БутылкиЕГАИС\"(\"Коробка\");";
                    idxCommand.ExecuteNonQuery();

                    idxCommand.CommandText = "CREATE INDEX \"SEARCH_IDX_БутылкиЕГАИС2\" ON \"БутылкиЕГАИС\"(\"Палета\");";
                    idxCommand.ExecuteNonQuery();

                    idxCommand.CommandText = "CREATE UNIQUE INDEX \"UNK_IDX_БутылкиЕГАИС\" ON \"БутылкиЕГАИС\"(\"АлкоПДФ\");";
                    idxCommand.ExecuteNonQuery();

                    idxCommand.CommandText = "CREATE UNIQUE INDEX \"UNK_IDX_БутылкиЕГАИС__UID__\" ON \"БутылкиЕГАИС\"(\"__UID__\");";
                    idxCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw new Exception("Ошибка построения индексов, изменения не записаны! " + ex.Message);
                }
                finally
                {
                    transaction.Commit();
                    sw.Stop();
                    Console.WriteLine($"Потрачено: {sw.Elapsed}");
                }
            }

            connection.Close();

            Console.WriteLine($"Готово.");
            Console.ReadKey();
        }

        public static SQLiteConnection CreateConnection(string db, params string[] args)
        {
            try
            {
                var connectionString = string.Format("Data source={0};{1}", db , string.Join(";", args));
                return new SQLiteConnection(connectionString);
            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка подключения к базе, возможно файл занят" + ex.Message);
            }
        }

        public static IEnumerable<Line> GenerateLines()
        {
            var iterator = 0;
            var boxBarcodePref = "(00)07777777";
            var chBoxCount = 1000;

            var palBarcodePref = "(00)17777777";
            var chPalCount = 5000;

            var i = 0;
            var boxBarcode = string.Format("{0}{1:0000000000}", boxBarcodePref, iterator);
            var palBarcode = string.Format("{0}{1:0000000000}", palBarcodePref, iterator);

            while (true)
            {
                var line = new Line();
                line.PalletBarcode = palBarcode;
                line.BoxBarcode = boxBarcode;
                yield return line;

                i++;
                if (i % chBoxCount == 0)
                {
                    iterator++;
                    boxBarcode = string.Format("{0}{1:0000000000}", boxBarcodePref, iterator);

                    if (i % chPalCount == 0)
                        palBarcode = string.Format("{0}{1:0000000000}", palBarcodePref, iterator);
                }
                else if (i % chPalCount == 0)
                {
                    iterator++;
                    palBarcode = string.Format("{0}{1:0000000000}", palBarcodePref, iterator);
                }
            }
        }
        
        public class Line
        {
            public string Uid { get; set; }
            public string AlcoPDF { get; set; }
            public string BoxBarcode { get; set; }
            public string PalletBarcode { get; set; }

            public Line()
            {
                Uid = Guid.NewGuid().ToString();
                AlcoPDF = Uid;
            }

            public override string ToString()
            {
                return $"('{Uid}', '{AlcoPDF}', '{BoxBarcode}', '{PalletBarcode}')";
            }
        }
    }
}
