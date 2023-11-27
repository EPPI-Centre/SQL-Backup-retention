using System;
using Microsoft.Extensions.Configuration;
using System.IO;
using Azure.Storage.Blobs;
using System.ComponentModel;
using Azure.Storage.Blobs.Models;
using System.Text.RegularExpressions;
using System.Reflection.Metadata;
using System.Globalization;
using Microsoft.VisualBasic;
using System.Security;

namespace SQLBackupRetention
{
    class Program
    {
        private static readonly DateTime Now = DateTime.Now.Date.AddDays(0);
        private static DateTime KeepAllCutoff = DateTime.Now;
        private static DateTime WeeksCutoff = DateTime.Now;
        private static DateTime MonthsCutoff = DateTime.Now;
        private static DateTime GlobalCutoff = DateTime.Now;
        private static string LogFileName = "";
        private static GeneralConfig Config = new GeneralConfig();
        private static List<BackedUpDatabases> backedUpDatabases = new List<BackedUpDatabases>();
        private static BackedUpDatabases CurrentDatabase = new BackedUpDatabases();
        private static string ConnStFullBacups = "";

        static void Main(string[] args)
        {
            try
            {
                MakeLogFile();
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR creating log file, will abort. Error message is below.");
                Console.WriteLine(ex.ToString());
                return;
            }
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true);

            var tconfiguration = builder.Build();

            var tcg = tconfiguration.GetSection("General").Get<GeneralConfig>();

            if (tcg == null)
            {
                LogLine("ERROR: Missing General configuration, or configuration file (appsettings.json) is invalid, aborting;");
                return;
            }
            else
            {
                Config = tcg;
            }

            var tcb = tconfiguration.GetSection("BackedUpDatabases").Get<List<BackedUpDatabases>>();
            if (tcb == null)
            {
                LogLine("ERROR: Missing \"BackedUpDatabases\" configuration section, or configuration file (appsettings.json) is invalid, aborting;");
                return;
            }
            else
            {
                backedUpDatabases = tcb;
            }
            //things look OK we can log, so we can now wrap everything in a nice try catch
            try
            {
                Config.CheckConfigIsValid();
                foreach (BackedUpDatabases bud in backedUpDatabases)
                {
                    bud.CheckConfigIsValid();
                }
                if (Config.VerboseLogging)
                {
                    LogLine();
                    LogLine("Running with values (verbose):");
                    LogLine("StorageURI: " + Config.StorageURI);
                    LogLine("DoNotUseBlobTimestamps: " + Config.DoNotUseBlobTimestamps.ToString());
                    LogLine("Running in \"simulation\" (AsIf) mode: " + Config.AsIf.ToString());
                    LogLine("Number of databases to process: " + backedUpDatabases.Count.ToString());
                    LogLine();
                }
                else
                {
                    LogLine("Running with minimal logging. Number of databases to process: " + backedUpDatabases.Count.ToString());
                }
                ConnStFullBacups = "SharedAccessSignature=" + Config.SAS + ";BlobEndpoint=" + Config.StorageURI + ";";
                foreach (BackedUpDatabases bud in backedUpDatabases)
                {
                    DoOneDatabase(bud);
                }
                LogLine("Exiting without errors.");


            }
            catch (Exception ex)
            {
                LogException(ex);
                LogLine("Error encountered, aborting.");
                return;
            }

        }
        private static void DoOneDatabase(BackedUpDatabases bud)
        {
            CurrentDatabase = bud;
            LogLine();
            LogLine("Processing folder: '" + Config.StorageURI + bud.BackupsContainer 
                + "'. Looking for backup files of database '" + bud.DatabaseName + "'.");
            BlobContainerClient containerFB = new BlobContainerClient(ConnStFullBacups, bud.BackupsContainer.ToLower());
            var blobs = containerFB.GetBlobs();
            List<BlobFile> AllFiles = new List<BlobFile>();
            //we will get and consider only files with the expected shape of filename:
            //starts with the name of the database, ends with ".bak" or ".trn"
            //all case-insensitive
            foreach (BlobItem blob in blobs)
            {
                string name = blob.Name.TrimEnd().ToLower();
                if (name.StartsWith(bud.DatabaseName.ToLower())
                    &&
                    (name.EndsWith(".bak")
                    || name.EndsWith(".trn"))
                    )
                {
                    BlobFile t = new BlobFile(blob);
                    AllFiles.Add(t);
                }
                else if (Config.VerboseLogging)
                {
                    LogLine("Skipping file '" + blob.Name + "'.");
                }
            }
            AllFiles.Sort();
            if (Config.VerboseLogging)
            {
                LogLine("All Files found (sorted by date):");
                foreach (BlobFile file in AllFiles)
                {
                    LogLine(file.FileName + " (" + file.CreationTime.ToString("dd MMM yyyy") + ")");
                }
            }
            //now, let's find what needs deleting...

            List<BlobFile> ToDo = AllFiles.FindAll(f => f.FileName.Trim().ToLower().StartsWith(CurrentDatabase.DatabaseName.Trim().ToLower() + "_"));
            if (ToDo.Any())
            {
                if (Config.VerboseLogging) LogLine("Processing files for '" + CurrentDatabase.DatabaseName.Trim().ToLower() + "'. N. of files is:" + ToDo.Count.ToString() + ".");
                else LogLine("Processing files for '" + CurrentDatabase.DatabaseName.Trim().ToLower() + "'.");
                ProcessSingleDatabase(ToDo, CurrentDatabase.IsStriped);
            }
            else
            {
                LogLine("No files found for '" + CurrentDatabase.DatabaseName.Trim().ToLower() + "' DB.");
            }

        }

        private static Regex Last1or2digitsInFilename = new Regex("\\d{1,2}", RegexOptions.RightToLeft);
        private static void ProcessSingleDatabase(List<BlobFile> AllFiles, bool isStriped = false)
        {
            //compute cutoff vals
            KeepAllCutoff = Now.AddDays(-CurrentDatabase.RetainAllInDays);
            WeeksCutoff = KeepAllCutoff.AddDays(CurrentDatabase.WeeksRetention * -7);
            MonthsCutoff = KeepAllCutoff.AddMonths(-CurrentDatabase.MonthsRetention);
            GlobalCutoff = WeeksCutoff <= MonthsCutoff ? WeeksCutoff : MonthsCutoff;
            if (Config.VerboseLogging)
            {
                LogLine();
                LogLine("Running with values:");
                LogLine("Retain all files from the last N days: " + CurrentDatabase.RetainAllInDays);
                LogLine("Will retain all files from " + KeepAllCutoff.ToString("dd MMM yyyy") + " onwards");
                LogLine("Retain 1 '.bak' file per additional N weeks: " + CurrentDatabase.WeeksRetention);
                LogLine("Will retain 1 '.bak' file per week, from: " + WeeksCutoff.ToString("dd MMM yyyy"));
                LogLine("Retain 1 '.bak' file per additional N months: " + CurrentDatabase.MonthsRetention);
                LogLine("Will retain 1 '.bak' file per month, from: " + MonthsCutoff.ToString("dd MMM yyyy"));
                LogLine();
            }
            if (Config.VerboseLogging && isStriped) LogLine("Database is flagged as 'striped' (has multiple '.bak' files per full backup).");
            
            List<BlobFile> FilesToDelete = new List<BlobFile>();
            List<BlobFile> WeekFilesToKeep = new List<BlobFile>();
            List<BlobFile> KeepAllFiles = new List<BlobFile>();
            List<BlobFile> MonthFilesToKeep = new List<BlobFile>();

            //let's calculate the intervals...
            List<TimeInterval> WeeksTI = new List<TimeInterval>();
            List<TimeInterval> MonthsTI = new List<TimeInterval>();
            for (int i = 0; i < CurrentDatabase.WeeksRetention; i++)
            {
                WeeksTI.Add(new TimeInterval()
                {
                    End = KeepAllCutoff.AddDays(i * -7),
                    Start = KeepAllCutoff.AddDays((i + 1) * -7)
                });
            }
            for (int i = 0; i < CurrentDatabase.MonthsRetention; i++)
            {
                MonthsTI.Add(new TimeInterval()
                {
                    End = KeepAllCutoff.AddMonths(-i),
                    Start = KeepAllCutoff.AddMonths(-(i + 1))
                });
            }
            if (Config.VerboseLogging)
            {
                LogLine("Weekly timespans for retaining one backup set:");
                foreach (TimeInterval ti in WeeksTI) LogLine("From " + ti.Start.ToString("dd MMM yyyy") 
                    + " included, to " + ti.End.ToString("dd MMM yyyy") + " excluded");
                LogLine("Monthly timespans for retaining one backup set:");
                foreach (TimeInterval ti in MonthsTI) LogLine("From " + ti.Start.ToString("dd MMM yyyy")
                    + " included, to " + ti.End.ToString("dd MMM yyyy") + " excluded");
            }
            //we don't keep transaction files outside the "keep all" period
            FilesToDelete.AddRange(AllFiles.FindAll(f => f.CreationTime < KeepAllCutoff && f.FileName.EndsWith(".trn")));
            //not keeping files that are older than the smaller possible cutoff date
            FilesToDelete.AddRange(AllFiles.FindAll(f => f.CreationTime < GlobalCutoff));

            foreach (BlobFile file in FilesToDelete)
            {
                AllFiles.Remove(file);
            }
            KeepAllFiles.AddRange(AllFiles.FindAll(f => f.CreationTime >= KeepAllCutoff));
            foreach (BlobFile file in KeepAllFiles)
            {
                AllFiles.Remove(file);
            }
            //we've removed all files that are older than our overall retention, or inside the "keep all"
            //we now need to put files inside our TimeInterval objects, for week and months
            foreach(TimeInterval ti in WeeksTI)
            {
                List<BlobFile> eligible = AllFiles.FindAll(f => f.CreationTime < ti.End && f.CreationTime >= ti.Start);
                //foreach (BlobFile b in eligible) Console.WriteLine("From " + ti.Start.ToString("dd MMM yyyy")
                //    + " included, to " + ti.End.ToString("dd MMM yyyy") + " excluded " + b.FileName);
                if (eligible.Any())
                {
                    BlobFile oneToAdd = eligible.Last();//the last eligible is the newest file in the interval
                    ti.BlobsToKeep.Add(oneToAdd);
                    eligible.Remove(oneToAdd);
                    if (CurrentDatabase.IsStriped)
                    {
                        //we'll try to find all stripes at this point...
                        Match match = Last1or2digitsInFilename.Match(oneToAdd.FileName);
                        if (match.Success)
                        {
                            string ConstantPartOfFilename = oneToAdd.FileName.Substring(0, match.Index);
                            List<BlobFile> matching = eligible.FindAll(f => f.FileName.StartsWith(ConstantPartOfFilename)
                                && f.CreationTime == oneToAdd.CreationTime);
                            foreach (BlobFile file in matching)
                            {
                                if (!ti.BlobsToKeep.Contains(file))
                                {
                                    ti.BlobsToKeep.Add(file);
                                }
                            }
                        }
                    }
                    WeekFilesToKeep.AddRange(ti.BlobsToKeep);
                }
            }
            //and again, for months
            foreach (TimeInterval ti in MonthsTI)
            {
                List<BlobFile> eligible = AllFiles.FindAll(f => f.CreationTime < ti.End && f.CreationTime >= ti.Start);
                //foreach (BlobFile b in eligible) Console.WriteLine("From " + ti.Start.ToString("dd MMM yyyy")
                //    + " included, to " + ti.End.ToString("dd MMM yyyy") + " excluded " + b.FileName);
                if (eligible.Any())
                {
                    BlobFile oneToAdd = eligible.Last();//the last eligible is the newest file in the interval
                    ti.BlobsToKeep.Add(oneToAdd);
                    eligible.Remove(oneToAdd);
                    if (CurrentDatabase.IsStriped)
                    {
                        //we'll try to find all stripes at this point...
                        Match match = Last1or2digitsInFilename.Match(oneToAdd.FileName);
                        if (match.Success)
                        {
                            string ConstantPartOfFilename = oneToAdd.FileName.Substring(0, match.Index);
                            List<BlobFile> matching = eligible.FindAll(f => f.FileName.StartsWith(ConstantPartOfFilename)
                                && f.CreationTime == oneToAdd.CreationTime);
                            foreach (BlobFile file in matching)
                            {
                                if (!ti.BlobsToKeep.Contains(file))
                                {
                                    ti.BlobsToKeep.Add(file);
                                }
                            }
                        }
                    }
                    MonthFilesToKeep.AddRange(ti.BlobsToKeep);
                }
            }
            //finally, deal with leftovers - there may still be files in "AllFiles"
            //but our lists of files to keep are now done, so whatever isn't in there, needs to move to the list for deletion
            for (int i = 0; i < AllFiles.Count; i++)
            {
                BlobFile file = AllFiles[i];
                if (!KeepAllFiles.Contains(file)
                    &&!WeekFilesToKeep.Contains(file)
                    &&!MonthFilesToKeep.Contains(file)) 
                {
                    AllFiles.Remove(file);
                    FilesToDelete.Add(file);
                    i--;
                }
            }

            //that's basically all done.
            //we have 4 lists: "to delete", "in the keep all range", "in weekly sets", in "monthly sets"
            //we can "just" delete what's in the former, but to be extra sure, we'll check that no file to delete is in the "to keep" ranges.
            foreach (BlobFile file in FilesToDelete)
            {
                if (KeepAllFiles.Contains(file)) throw new Exception("Bug alert! A file marked for deletion is also in the \"Keep all\" range. Aborting.");
                else if (WeekFilesToKeep.Contains(file)) throw new Exception("Bug alert! A file marked for deletion is also in one of the \"Weekly\" sets. Aborting.");
                else if (MonthFilesToKeep.Contains(file)) throw new Exception("Bug alert! A file marked for deletion is also in one of the \"Monthly\" sets. Aborting.");
            }

            LogLine("Summary for \"" + CurrentDatabase.DatabaseName + "\":");
            if (Config.VerboseLogging)
            {
                if (Config.AsIf == true) LogLine("AsIf flag is 'true', running in simulation mode - WILL NOT DELETE ANYTHING!");
                if (Config.AsIf == false) LogLine("Deleting (" + FilesToDelete.Count.ToString() + "):");
                else LogLine("Would delete (" + FilesToDelete.Count.ToString() + "):");
                foreach (BlobFile file in FilesToDelete) LogLine(file.FileName + " - " + file.CreationTime.ToString("dd MMM yyyy"));
                LogLine();
                if (Config.AsIf == false) LogLine("Files kept in the \"Keep All\" range (" + KeepAllFiles.Count.ToString() + ") :");
                else LogLine("Files that would be kept in the \"Keep All\" range (" + KeepAllFiles.Count.ToString() + ") :");
                foreach (BlobFile file in KeepAllFiles) LogLine(file.FileName + " - " + file.CreationTime.ToString("dd MMM yyyy"));
                LogLine();
                if (Config.AsIf == false) LogLine("Weekly files kept (" + WeekFilesToKeep.Count.ToString() + "):");
                else LogLine("Weekly files that would be kept (" + WeekFilesToKeep.Count.ToString() + "):");
                foreach (BlobFile file in WeekFilesToKeep) LogLine(file.FileName + " - " + file.CreationTime.ToString("dd MMM yyyy"));
                LogLine();
                if (Config.AsIf == false) LogLine("Monthly files kept (" + MonthFilesToKeep.Count.ToString() + "):");
                else LogLine("Monthly files that would be kept (" + MonthFilesToKeep.Count.ToString() + "):");
                foreach (BlobFile file in MonthFilesToKeep) LogLine(file.FileName + " - " + file.CreationTime.ToString("dd MMM yyyy"));
                LogLine();
            }
            else
            {
                if (Config.AsIf == false) LogLine("Deleting: " + FilesToDelete.Count.ToString() + " files.");
                else LogLine("Running in 'As if' mode, would otherwise delete: " + FilesToDelete.Count.ToString() + " files.");
            }

            if (Config.AsIf == false) DeleteTheseFiles(FilesToDelete);
        }
        private static void DeleteTheseFiles(List<BlobFile> FilesToDelete)
        {
            BlobContainerClient containerFB = new BlobContainerClient(ConnStFullBacups, CurrentDatabase.BackupsContainer.ToLower());

            foreach (BlobFile file in FilesToDelete)
            {
                try
                {
                    containerFB.GetBlobClient(file.FileName).DeleteIfExists();
                }
                catch (Exception e)
                {
                    LogException(e, "Failed to delete file: " + file.FileName + ". Will try deleting the remaining files.");
                }
            }
        }
        private static void MakeLogFile()
        {
            DirectoryInfo logDir = System.IO.Directory.CreateDirectory("LogFiles");
            string tLogFilename = logDir.FullName + @"\" + "SQLBackupRetention-" + DateTime.Now.ToString("dd-MM-yyyy") + ".txt";
            if (!System.IO.File.Exists(tLogFilename))
            {
                using (FileStream fs = System.IO.File.Create(tLogFilename))
                {
                    fs.Close();
                }
            }
            LogFileName = tLogFilename;
            LogLine();
            LogLine("----------------------------------");
            LogLine("Starting SQLBackupRetention app...");
            LogLine();
        }
        private static void LogException(Exception ex, string message = "")
        {
            if (message != "") LogLine("ERROR >>> " + message);
            if (ex.Message != null && ex.Message != "")
                LogLine("MSG: " + ex.Message);
            if (ex.StackTrace != null && ex.StackTrace != "")
                LogLine("STACK TRC:" + ex.StackTrace);
            if (ex.InnerException != null)
            {
                LogLine("Inner Exception(s): ");
                Exception? ie = ex.InnerException;
                int i = 0;
                while (ie != null && i < 10)
                {
                    i++;
                    if (ie.Message != null && ie.Message != "")
                        LogLine("MSG(" + i.ToString() + "): " + ie.Message);
                    if (ie.StackTrace != null && ie.StackTrace != "")
                        LogLine("STACK TRC(" + i.ToString() + "):" + ie.StackTrace);
                    ie = ie.InnerException;
                }
            }
            LogLine();
        }

        private static void LogLine(string Line = "")
        {
            try
            {
                using (StreamWriter f = new FileInfo(LogFileName).AppendText())
                {
                    Console.WriteLine(Line);
                    f.WriteLine(Line);
                }
            }
            catch
            {
                //do nothing, can't log failures to log!
            }
        }
        //2023_11_22
        private static Regex FindTimeStampRx = new Regex("_\\d\\d\\d\\d_\\d\\d_\\d\\d_");
        private class BlobFile : IComparable<BlobFile>
        {
            public int CompareTo(BlobFile? other)
            {
                if (other == null) return CreationTime.CompareTo(null);
                else return CreationTime.CompareTo(other.CreationTime);
            }
            public string FileName { get; private set; } = "";
            public DateTime CreationTime { get; private set; } = DateTime.Now.AddDays(1);//if we can't get the time of a file, we won't delete it (timestamp in the future)
            public BlobItem blobItem { get; private set; }
            public BlobFile(BlobItem blob)
            {
                blobItem = blob;
                FileName = blob.Name;
                Match m = FindTimeStampRx.Match(FileName);
                DateTime tmp = DateTime.MinValue;
                if (m.Success)
                {
                    if (DateTime.TryParseExact(m.Value, "_yyyy_MM_dd_", new CultureInfo("en-US"), DateTimeStyles.None, out tmp))
                        CreationTime = (DateTime)tmp;
                    //Console.WriteLine(tmp.ToString());
                }
                if (tmp == DateTime.MinValue && Config.DoNotUseBlobTimestamps == false)
                {//we'll use the blob timestamp
                    if (blob.Properties.CreatedOn != null)
                        CreationTime = blob.Properties.CreatedOn.Value.DateTime;
                    // nothing else, if we can't find the timestamp of a file, we'll let it be - already has a date in the future...
                }
            }
        }
        private class GeneralConfig
        {
            public string StorageURI { get; set; } = "";
            public string SAS { get; set; } = "";
            public bool VerboseLogging { get; set; } = true;
            public bool DoNotUseBlobTimestamps { get; set; } = true;
            public bool AsIf { get; set; } = true;

            public void CheckConfigIsValid()
            {
                if (!StorageURI.StartsWith("https://"))
                    throw new InvalidDataException("StorageURI configuration value is not valid, needs to be present and to start with \"https://\"");
                else if (SAS == "" || !SAS.StartsWith("sv="))
                    throw new InvalidDataException("SAS configuration value is not valid, it is either absent or invalid.");
            }
        }
        private class BackedUpDatabases
        {
            public string BackupsContainer { get; set; } = "";
            //public string TransactionLogsContainer { get; set; } = "";
            public string DatabaseName { get; set; } = "";
            public bool IsStriped { get; set; } = true;
            public int RetainAllInDays { get; set; } = -1;
            public int WeeksRetention { get; set; } = -1;
            public int MonthsRetention { get; set; } = -1;
            public void CheckConfigIsValid()
            {
                BackupsContainer = BackupsContainer.ToLower();
                DatabaseName = DatabaseName.ToLower();
                if (BackupsContainer == "")
                    throw new InvalidDataException("A BackupsContainer configuration value is not valid, needs to be present.");
                //else if (TransactionLogsContainer == "")
                //    throw new InvalidDataException("A TransactionLogsContainer configuration value is not valid, needs to be present.");
                else if (DatabaseName == "")
                    throw new InvalidDataException("A DatabaseName configuration value is not valid, needs to be present.");
                else if (RetainAllInDays < 0)
                    throw new InvalidDataException("RetainAllInDays configuration value for Database \""
                        + DatabaseName + "\" is not valid, needs to be a positive integer (including zero).");
                else if (WeeksRetention < 0)
                    throw new InvalidDataException("WeeksRetention configuration value for Database \""
                        + DatabaseName + "\" is not valid, needs to be a positive integer (including zero).");
                else if (MonthsRetention < 0)
                    throw new InvalidDataException("MonthsRetention configuration value for Database \""
                        + DatabaseName + "\" is not valid, needs to be a positive integer (including zero).");
            }

        }

        private class TimeInterval
        {
            public DateTime Start { get; set; } = DateTime.MinValue;
            public DateTime End { get; set; } = DateTime.MinValue;
            public List<BlobFile> BlobsToKeep { get; set; } = new List<BlobFile>();
            public bool HasFiles { get { return BlobsToKeep.Count > 0; } }
        }
    }
}