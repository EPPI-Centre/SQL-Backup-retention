using System;
using Microsoft.Extensions.Configuration;
using System.IO;
using Azure.Storage.Blobs;
using System.ComponentModel;
using Azure.Storage.Blobs.Models;

namespace SQLBackupRetention
{
    class Program
    {
        static void Main(string[] args)
        {

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true);

            var configuration = builder.Build();

            string? tStorageURI = configuration["StorageURI"];
            string StorageURI = String.IsNullOrEmpty(tStorageURI) ? "" : tStorageURI.ToString();
            string? tSAS = configuration["SAS"];
            string SAS = String.IsNullOrEmpty(tSAS) ? "" : tSAS.ToString();
            string? tFullBackupsContainer = configuration["FullBackupsContainer"];
            string FullBackupsContainer = String.IsNullOrEmpty(tFullBackupsContainer) ? "" : tFullBackupsContainer.ToString();
            string? tTransactionLogsContainer = configuration["TransactionLogsContainer"];
            string TransactionLogsContainer = String.IsNullOrEmpty(tTransactionLogsContainer) ? "" : tTransactionLogsContainer.ToString();

            if (String.IsNullOrEmpty(StorageURI)
                || String.IsNullOrEmpty(tSAS)
                || String.IsNullOrEmpty(FullBackupsContainer)
                || String.IsNullOrEmpty(TransactionLogsContainer))
            {
                throw new Exception("Missing at least one parameter");
            }


            string ConnStFullBacups = "SharedAccessSignature=" + SAS + ";BlobEndpoint=" + tStorageURI +";";
               BlobContainerClient containerFB = new BlobContainerClient(ConnStFullBacups, FullBackupsContainer);
            var blobs = containerFB.GetBlobs();
            List<BlobItem> AllFiles = new List<BlobItem>();
            foreach (BlobItem blob in blobs)
            {
                AllFiles.Add(blob);
                Console.WriteLine($"{blob.Name} --> Created On: {blob.Properties.CreatedOn:yyyy-MM-dd HH:mm:ss}  Size: {blob.Properties.ContentLength}");
            }

        }
    }
}