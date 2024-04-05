using FinancialTrackingSystemMain.Model;
using FinancialTrackingSystemMain.Model.Tenant;
using Newtonsoft.Json;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinancialTrackingSystemMain.Service
{
    public class Validator
    {
        private TenantSetting ReadTenantJsonfile(string path, string tenantId)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("JSON file not found: ", path);
            string json = File.ReadAllText(path);
            var tenantData = JsonConvert.DeserializeObject<dynamic>(json);
            // cannot use linq query due to dynamic type object
            var tenantSettingsList = tenantData?.tenantsettings;
            if (tenantSettingsList != null)
            {
                foreach(var tenantSetting in tenantSettingsList)
                {
                    if(tenantSetting.tenantid == tenantId)
                        return JsonConvert.DeserializeObject<TenantSetting>(tenantSetting.ToString());
                }
            }
            return null;
        }
        public bool Validate(Transaction transaction)
        {
            if (transaction == null)
                throw new ArgumentNullException();
            string tenantId = transaction.tenantId;
            string absPath = Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.FullName, "data", "tenants_settings.json");
            TenantSetting tenantSetting = ReadTenantJsonfile(absPath, tenantId);
            if (tenantSetting == null)
                return false;
            // compare the velocity limit
            decimal transactionAmount = Decimal.Parse(transaction.amount);
            decimal velocityLimit = Decimal.Parse(tenantSetting?.velocitylimits?.daily);
            if (transactionAmount > velocityLimit)
            {
                return false;
            }
            var threshold = Decimal.Parse(tenantSetting?.thresholds?.pertransaction);
            if (transactionAmount > threshold)
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
        }
    }
}
