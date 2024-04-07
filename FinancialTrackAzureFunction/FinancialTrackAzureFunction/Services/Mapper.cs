using FinancialTrackAzureFunction.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction.Services
{
    public class Mapper
    {
        public static Transaction GetDeserializedTransaction(string serializedTransaction)
        {
            Transaction transaction = JsonSerializer.Deserialize<Transaction>(serializedTransaction);
            return transaction;
        }
    }
}
