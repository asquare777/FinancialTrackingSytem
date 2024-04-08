using FinancialTrackAzureFunction.Models;
using FinancialTrackAzureFunction.Models.Tenant;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction.Services
{
    public class Validator
    {
        private readonly string path;
        public Validator()
        {
            path = Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.FullName, "data", "tenants_settings.json");
        }

        private TenantSetting ReadTenantJsonfile(string tenantId)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("JSON file not found: ", path);
            string json = File.ReadAllText(path);
            var tenantData = JsonConvert.DeserializeObject<dynamic>(json);
            // cannot use linq query due to dynamic type object
            var tenantSettingsList = tenantData?.tenantsettings;
            if (tenantSettingsList != null)
            {
                foreach (var tenantSetting in tenantSettingsList)
                {
                    if (tenantSetting.tenantid == tenantId)
                        return JsonConvert.DeserializeObject<TenantSetting>(tenantSetting.ToString());
                }
            }
            return null;
        }

        private async Task<bool> IsValidVelocityTransaction(string tenantId, decimal transactionAmount, string storedVelocityLimit, IDurableEntityClient entityClient)
        {
            try
            {
                var entityId = new EntityId("VelocityLimitEntity", tenantId);
                decimal velocityLimit = Decimal.Parse(storedVelocityLimit);
                var stateResponse = await entityClient.ReadEntityStateAsync<decimal>(entityId);
                if (stateResponse.EntityExists)
                    velocityLimit = stateResponse.EntityState;
                if (transactionAmount < velocityLimit)
                {
                    decimal NewVelocityLimit = velocityLimit - transactionAmount;
                    await entityClient.SignalEntityAsync(entityId, "set", NewVelocityLimit);
                    return true;
                }
                return false;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }

        public async Task<bool> Validate(Transaction transaction, IDurableEntityClient entityClient)
        {
            try
            {
                if (transaction == null)
                    throw new ArgumentNullException();
                string tenantId = transaction.tenantId;
                TenantSetting tenantSetting = ReadTenantJsonfile(tenantId);
                if (tenantSetting == null)
                    return false;
                // compare the velocity limit
                decimal transactionAmount = Decimal.Parse(transaction.amount);
                var threshold = Decimal.Parse(tenantSetting?.thresholds?.pertransaction);
                if (transactionAmount > threshold)
                    return false;
                bool isValidVelocityTransaction = await IsValidVelocityTransaction(transaction.tenantId, transactionAmount, tenantSetting?.velocitylimits?.daily, entityClient);
                if (!isValidVelocityTransaction)
                    return false;
                var tenantSettingSanctions = tenantSetting.countrysanctions;
                var sansctionsSources = tenantSettingSanctions.sourcecountrycode.Split(",");
                var sansctionsDestinations = tenantSettingSanctions.destinationcountrycode.Split(",");
                var transactionSource = transaction.sourceaccount?.countrycode;
                var transactionDestination = transaction.destinationaccount?.countrycode;
                if (sansctionsSources.Any(source => transactionSource.Trim().ToLower() == source.Trim().ToLower()))
                    return false;
                if (sansctionsDestinations.Any(source => transactionDestination.Trim().ToLower() == source.Trim().ToLower()))
                    return false;
                return true;
            }catch(Exception ex)
            {
                throw ex;
            }
        }
    }
}
