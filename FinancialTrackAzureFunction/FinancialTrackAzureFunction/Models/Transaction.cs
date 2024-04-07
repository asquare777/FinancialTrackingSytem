using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction.Models
{
    public class Transaction
    {
        public string correlationId { get; set; }
        public string tenantId { get; set; }
        public string transactionId { get; set; }
        public string transactionDate { get; set; }
        public string direction { get; set; }
        public string amount { get; set; }
        public string currency { get; set; }
        public string description { get; set; }
        public Account sourceaccount { get; set; }
        public Account destinationaccount { get; set; }
    }
}
