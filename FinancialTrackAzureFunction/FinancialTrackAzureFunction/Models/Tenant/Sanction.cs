using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction.Models.Tenant
{
    public class Sanction
    {
        public string sourcecountrycode { get; set; }
        public string destinationcountrycode { get; set; }
    }
}
