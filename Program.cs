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

namespace SQLBackupRetention
{
    class Program
    {
        private static readonly DateTime Now = DateTime.Now.Date.AddDays(1);
        private static DateTime KeepAllCutoff = DateTime.Now;
        private static DateTime WeeksCutoff = DateTime.Now;
        private static DateTime MonthsCutoff = DateTime.Now;
        private static DateTime GlobalCutoff = DateTime.Now;
        private static string LogFileName = "";
        private static ConfigData Config = new ConfigData();
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

            Config = tconfiguration.Get<ConfigData>();
            if (Config == null)
            {
                LogLine("ERROR: Missing configuration, or configuration file (appsettings.json) is invalid, aborting;");
                return;
            }
            //things look OK we can log, so we can now wrap everything in a nice try catch
            try
            {
                string ConfigValid = Config.CheckConfigIsValid();
                if (ConfigValid != "")
                {
                    LogLine(ConfigValid);
                }

                //compute cutoff vals
                KeepAllCutoff = Now.AddDays(-Config.RetainAllInDays);
                WeeksCutoff = KeepAllCutoff.AddDays(Config.WeeksRetention * -7);
                MonthsCutoff = KeepAllCutoff.AddMonths(-Config.MonthsRetention);
                GlobalCutoff = WeeksCutoff <= MonthsCutoff ? WeeksCutoff : MonthsCutoff;
                if (Config.VerboseLogging)
                {
                    LogLine();
                    LogLine("Running with vals:");
                    LogLine("Retain all files from the last N days: " + Config.RetainAllInDays);
                    LogLine("Will retain all files from " + KeepAllCutoff.ToString("dd MMM yyyy") + " onwards");
                    LogLine("Retain 1 '.bak' file per addittonal N weeks: " + Config.WeeksRetention);
                    LogLine("Will retain 1 '.bak' file per week, from: " + WeeksCutoff.ToString("dd MMM yyyy"));
                    LogLine("Retain 1 '.bak' file per addittonal N months: " + Config.MonthsRetention);
                    LogLine("Will retain 1 '.bak' file per month, from: " + MonthsCutoff.ToString("dd MMM yyyy"));
                    LogLine();
                    LogLine();
                }

                ConnStFullBacups = "SharedAccessSignature=" + Config.SAS + ";BlobEndpoint=" + Config.StorageURI + ";";
                BlobContainerClient containerFB = new BlobContainerClient(ConnStFullBacups, Config.FullBackupsContainer);
                var blobs = containerFB.GetBlobs();
                List<BlobFile> AllFiles = new List<BlobFile>();
                foreach (BlobItem blob in blobs)
                {
                    if (blob.Name.TrimEnd().ToLower().EndsWith(".bak")
                        || blob.Name.TrimEnd().ToLower().EndsWith(".trn"))
                    {
                        BlobFile t = new BlobFile(blob);
                        AllFiles.Add(t);
                    }
                    else if (Config.VerboseLogging)
                    {
                        LogLine("Skipping file '" + blob.Name + "', does not end in '.bak' or '.trn'.");
                    }
                }
                AllFiles.Sort();
                if (Config.VerboseLogging)
                {
                    LogLine("All Files (sorted by date):");
                    foreach (BlobFile file in AllFiles)
                    {
                        //LogLine(file.FileName + " (" + file.CreationTime.ToString("dd MMM yyyy") + ")");
                    }
                }
                //now, let's find what needs deleting...
                string[] DBs = Config.ListOfDatabaseNames.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                string[] stripedDBs = Config.ListOfDatabaseNames.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                foreach (string DB in DBs)
                {
                    List<BlobFile> ToDo = AllFiles.FindAll(f=> f.FileName.Trim().ToLower().StartsWith(DB.Trim().ToLower()));
                    if (ToDo.Any())
                    {
                        if (Config.VerboseLogging) LogLine("Processing files for '" + DB.Trim().ToLower() + "' DB. N. of files is:" + ToDo.Count.ToString() + ".");
                        else LogLine("Processing files for '" + DB.Trim().ToLower() + "'.");
                        ProcessSingleDatabase(ToDo, stripedDBs.Contains(DB));
                    } 
                    else 
                    {
                        LogLine("No files found for '" + DB.Trim().ToLower() + "' DB.");
                    }
                }
                
            }
            catch (Exception ex)
            {
                LogException(ex);
                LogLine("Error encountered, aborting.");
                return;
            }

        }
        private static Regex Last1or2digitsInFilename = new Regex("\\d{1,2}", RegexOptions.RightToLeft);
        private static void ProcessSingleDatabase(List<BlobFile> AllFiles, bool isStriped = false)
        {
            if (Config.VerboseLogging && isStriped) LogLine("Database is flagged as 'striped' (multiple '.bak' files per full backup).");
            List<BlobFile> FilesToDelete = new List<BlobFile>();
            List<BlobFile> WeekFilesToKeep = new List<BlobFile>();
            List<BlobFile> KeepAllFiles = new List<BlobFile>();
            List<BlobFile> MonthFilesToKeep = new List<BlobFile>();
            DateTime NextKeepWeek = KeepAllCutoff;
            DateTime NextKeepMonth = KeepAllCutoff;
            bool currentMonthDone = false; int totMonthsDone = 0;
            bool currentWeekDone = false; int totWeeksDone = 0;
            FilesToDelete.AddRange(AllFiles.FindAll(f => f.CreationTime < KeepAllCutoff && f.FileName.EndsWith(".trn")));//not keeping and transaction files
            FilesToDelete.AddRange(AllFiles.FindAll(f => f.CreationTime < GlobalCutoff));//not keeping files that are older than the smaller possible cutoff date
            foreach (BlobFile file in FilesToDelete)
            {
                AllFiles.Remove(file);
            }
            KeepAllFiles.AddRange(AllFiles.FindAll(f => f.CreationTime >= KeepAllCutoff));
            foreach (BlobFile file in KeepAllFiles)
            {
                AllFiles.Remove(file);
            }
            for (int i = AllFiles.Count - 1; i >= 0; i--)
            {//from newer file to older, only files not in the "keep all range"
                BlobFile t = AllFiles[i];
                //considering the Weekly files only
                if (t.CreationTime >= NextKeepWeek)
                {
                    if (currentWeekDone)
                    {
                        if (!FilesToDelete.Contains(t) && !KeepAllFiles.Contains(t)
                            && !WeekFilesToKeep.Contains(t) && !MonthFilesToKeep.Contains(t)) FilesToDelete.Add(t);
                    }
                    else
                    {
                        currentWeekDone = true;
                        if (t.CreationTime >= WeeksCutoff && t.CreationTime >= GlobalCutoff)
                        {
                            if (isStriped == false) WeekFilesToKeep.Add(t);
                            else
                            {//we'll try to find all stripes at this point...
                                List<BlobFile> stripes = new List<BlobFile>();
                                stripes.Add(t);
                                Match match = Last1or2digitsInFilename.Match(t.FileName);
                                if (match.Success)
                                {
                                    string ConstantPartOfFilename = t.FileName.Substring(0, match.Index);
                                    List<BlobFile> matching = AllFiles.FindAll(f=> f.FileName.StartsWith(ConstantPartOfFilename)
                                        && f.CreationTime == t.CreationTime);
                                    foreach (BlobFile file in matching)
                                    {
                                        if (!stripes.Contains(file) && !WeekFilesToKeep.Contains(file))
                                        {
                                            stripes.Add(file);
                                            AllFiles.Remove(file);
                                            i--;//we found the first file in a series of stripes and we're removing a subsequent stripe file, making out i too big by one, so we correct it
                                        }
                                    }
                                }

                                WeekFilesToKeep.AddRange(stripes);
                            }
                        }
                        else if (!FilesToDelete.Contains(t) && !KeepAllFiles.Contains(t)
                            && !WeekFilesToKeep.Contains(t) && !MonthFilesToKeep.Contains(t)) FilesToDelete.Add(t);
                    }
                }
                else
                {//we slipped in the next week!
                    if (totWeeksDone < Config.WeeksRetention)
                    {//we want to add more weekly files
                        totWeeksDone++;
                        currentWeekDone = false;
                        NextKeepWeek = NextKeepWeek.AddDays(-7);
                        i++;//We do this same file again, will match previous cases
                        continue;
                    }
                }
                //now the monthly files...
                if (t.CreationTime >= NextKeepMonth)
                {
                    if (currentMonthDone)
                    {
                        if (!FilesToDelete.Contains(t) && !KeepAllFiles.Contains(t)
                            && !WeekFilesToKeep.Contains(t) && !MonthFilesToKeep.Contains(t)) FilesToDelete.Add(t);
                    }
                    else
                    {
                        currentMonthDone = true;
                        if (t.CreationTime >= MonthsCutoff && t.CreationTime >= GlobalCutoff)
                        {
                            if (isStriped == false) MonthFilesToKeep.Add(t);
                            else
                            {//we'll try to find all stripes at this point...
                                List<BlobFile> stripes = new List<BlobFile>();
                                stripes.Add(t);
                                Match match = Last1or2digitsInFilename.Match(t.FileName);
                                if (match.Success)
                                {
                                    string ConstantPartOfFilename = t.FileName.Substring(0, match.Index);
                                    List<BlobFile> matching = AllFiles.FindAll(f => f.FileName.StartsWith(ConstantPartOfFilename)
                                         && f.CreationTime == t.CreationTime);
                                    foreach (BlobFile file in matching)
                                    {
                                        if (!stripes.Contains(file) && !MonthFilesToKeep.Contains(file))
                                        {
                                            stripes.Add(file);
                                            AllFiles.Remove(file);
                                            i--;//we found the first file in a series of stripes and we're removing a subsequent stripe file, making out i too big by one, so we correct it
                                        }
                                    }
                                }
                                MonthFilesToKeep.AddRange(stripes);
                            }
                        }
                        else if (!FilesToDelete.Contains(t) && !KeepAllFiles.Contains(t)
                            && !WeekFilesToKeep.Contains(t) && !MonthFilesToKeep.Contains(t)) FilesToDelete.Add(t);
                    }
                }
                else if (totMonthsDone < Config.MonthsRetention)
                {//we want to add more monthly files
                    currentMonthDone = false;
                    totMonthsDone++;
                    NextKeepMonth = NextKeepMonth.AddMonths(-1);
                    i++;//We do this same file again, will match previous cases
                    continue;
                }
                else if (!FilesToDelete.Contains(t) && !KeepAllFiles.Contains(t)
                    && !WeekFilesToKeep.Contains(t) && !MonthFilesToKeep.Contains(t)) FilesToDelete.Add(t);

            }
            if (Config.VerboseLogging)
            {
                if (Config.AsIf == true) LogLine("AsIf flag is 'true', running in simulation mode - WILL NOT DELETE ANYTHING!");
                if (Config.AsIf == false) LogLine("Deleting (" + FilesToDelete.Count.ToString() + "):");
                else LogLine("Would delete (" + FilesToDelete.Count.ToString() + "):");
                foreach (BlobFile file in FilesToDelete) LogLine(file.FileName);
                LogLine();
                if (Config.AsIf == false) LogLine("Files kept in the \"Keep All\" range (" + KeepAllFiles.Count.ToString() + ") :");
                else LogLine("Files that would be kept in the \"Keep All\" range (" + KeepAllFiles.Count.ToString() + ") :");
                foreach (BlobFile file in KeepAllFiles) LogLine(file.FileName);
                LogLine();
                if (Config.AsIf == false) LogLine("Weekly files kept (" + WeekFilesToKeep.Count.ToString() + "):");
                else LogLine("Weekly files that would be kept (" + WeekFilesToKeep.Count.ToString() + "):");
                foreach (BlobFile file in WeekFilesToKeep) LogLine(file.FileName);
                LogLine();
                if (Config.AsIf == false) LogLine("Monthly files kept (" + MonthFilesToKeep.Count.ToString() + "):");
                else LogLine("Monthly files that would be kept (" + MonthFilesToKeep.Count.ToString() + "):");
                foreach (BlobFile file in MonthFilesToKeep) LogLine(file.FileName);
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
            BlobContainerClient containerFB = new BlobContainerClient(ConnStFullBacups, Config.FullBackupsContainer);

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
                Exception ie = ex.InnerException;
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
                using (StreamWriter f = new StreamWriter(LogFileName))
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
                        CreationTime =  blob.Properties.CreatedOn.Value.DateTime;
                    // nothing else, if we can't find the timestamp of a file, we'll let it be - already has a date in the future...
                }
            }
        }
        private class ConfigData
        {
            private string _StorageURI = ""; 
            private string _SAS = "";
            private string _FullBackupsContainer = "";
            private string _TransactionLogsContainer = "";
            private string _ListOfDatabaseNames = "";
            private string _ListOfStripedDatabases = "";
            private int _RetainAllInDays = -1;
            private int _WeeksRetention = -1; 
            private int _MonthsRetention = -1;
            private bool _AsIf = true;
            private bool _AsIfImplicit = true;
            public string StorageURI { 
                get { return _StorageURI; }
                set 
                {
                    _StorageURI = value;
                } 
            }
            public string SAS
            {
                get { return _SAS; }
                set
                {
                    _SAS = value;
                }
            }
            public string FullBackupsContainer
            {
                get { return _FullBackupsContainer; }
                set
                {
                    
                    _FullBackupsContainer = value;
                }
            }
            public string TransactionLogsContainer
            {
                get { return _TransactionLogsContainer; }
                set
                {
                    _TransactionLogsContainer = value;
                }
            }
            public string ListOfDatabaseNames
            {
                get { return _ListOfDatabaseNames; }
                set
                {
                    _ListOfDatabaseNames = value.Trim().ToLower();
                }
            }
            public string ListOfStripedDatabases
            {
                get { return _ListOfStripedDatabases; }
                set { _ListOfStripedDatabases=value.Trim().ToLower(); }
            }
            public int RetainAllInDays
            {
                get { return _RetainAllInDays; }
                set
                {
                    _RetainAllInDays = value;
                }
            }
            public int WeeksRetention
            {
                get { return _WeeksRetention; }
                set
                {
                    _WeeksRetention = value;
                }
            }
            public int MonthsRetention
            {
                get { return _MonthsRetention; }
                set
                {
                     _MonthsRetention = value;
                }
            }
            public bool AsIf
            {
                get { return _AsIf; }
                set
                {
                    _AsIfImplicit = false;
                    _AsIf = value;
                }
            }
            public bool DoNotUseBlobTimestamps { get; set; } = true;
            public bool VerboseLogging { get; set; } = true;
            public string CheckConfigIsValid()
            {
                if (_ListOfDatabaseNames == "") 
                    throw new ArgumentNullException("ListOfDatabaseNames configuration value is not valid, needs to be present! (please use a comma-separated list of names for multiple DBs)");
                else if(!StorageURI.StartsWith("https://")) 
                    throw new ArgumentNullException("StorageURI configuration value is not valid, needs to be present and to start with \"https://\"");
                else if (FullBackupsContainer.Length < 1)
                    throw new ArgumentNullException("FullBackupsContainer configuration value is not valid, needs to be present.");
                else if (TransactionLogsContainer.Length < 1)
                    throw new ArgumentNullException("TransactionLogsContainer configuration value is not valid, needs to be present.");
                else if (RetainAllInDays < 0)
                    throw new ArgumentNullException("RetainAllInDays configuration value is not valid, needs to be present and to be an integer with value >= 0.");
                else if (WeeksRetention  < 0)
                    throw new ArgumentNullException("WeeksRetention configuration value is not valid, needs to be present and to be an integer with value >= 0.");
                else if (MonthsRetention < 0)
                    throw new ArgumentNullException("MonthsRetention configuration value is not valid, needs to be present and to be an integer with value >= 0.");
                else if (_AsIfImplicit)
                {
                    return "AsIf setting was not explicitly set, therefore app is running with AsIf=true, to avoid accidental deletions.";
                }
                //we'll check the list of striped DBs if present...
                if (_ListOfStripedDatabases != "" )
                {
                    string[] DBs = _ListOfDatabaseNames.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                    string[] stripedDBs = _ListOfStripedDatabases.Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                    foreach(string striped in stripedDBs)
                    {
                        if (!DBs.Contains(striped))
                            throw new Exception("ListOfStripedDatabases configuration value is not valid, needs to include ONLY names mentioned in ListOfDatabaseNames.");
                    }
                }
                return "";
            }
        }
    }
}