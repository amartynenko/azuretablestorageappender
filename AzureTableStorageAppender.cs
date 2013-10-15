/*
Copyright 2011 Vidar Kongsli

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
    
Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
 */

using System.Data.Services.Client;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

using log4net.Appender;
using log4net.Core;

using Microsoft.WindowsAzure.ServiceRuntime;

namespace Demo.Log4Net.Azure
{
    public class AzureTableStorageAppender : AppenderSkeleton
    {
        public string TableStorageConnectionStringName { get; set; }
        private CloudTable table;

        public override void ActivateOptions()
        {
            base.ActivateOptions();

            var storageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue(TableStorageConnectionStringName));
            var tableClient = storageAccount.CreateCloudTableClient();

            table = tableClient.GetTableReference("LogEntries");
            table.CreateIfNotExists();
        }

        protected override void Append(LoggingEvent loggingEvent)
        {
            try
            {
                table.Execute(TableOperation.InsertOrReplace(new LogEntry
                {
                    RoleInstance = RoleEnvironment.CurrentRoleInstance.Id,
                    DeploymentId = RoleEnvironment.DeploymentId,
                    Timestamp = loggingEvent.TimeStamp,
                    Message = loggingEvent.RenderedMessage,
                    Level = loggingEvent.Level.Name,
                    LoggerName = loggingEvent.LoggerName,
                    Domain = loggingEvent.Domain,
                    ThreadName = loggingEvent.ThreadName,
                    Identity = loggingEvent.Identity
                }));
            }
            catch (DataServiceRequestException e)
            {
                ErrorHandler.Error(string.Format("{0}: Could not write log entry to Azure Table Storage: {1}",
                    GetType().AssemblyQualifiedName, e.Message));
            }
        }
    }
}