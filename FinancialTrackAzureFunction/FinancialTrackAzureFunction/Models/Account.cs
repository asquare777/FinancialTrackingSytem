using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction.Models
{
    public class Account
    {
        public string accountno { get; set; }
        public string sortcode { get; set; }
        public string countrycode { get; set; }
    }
}
