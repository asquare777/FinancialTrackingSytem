using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction.Models.Tenant
{
    public class TenantSetting
    {
        public string tenantid { get; set; }
        public VelocityLimit velocitylimits { get; set; }
        public Threshold thresholds { get; set; }
        public Sanction countrysanctions { get; set; }
    }
}
