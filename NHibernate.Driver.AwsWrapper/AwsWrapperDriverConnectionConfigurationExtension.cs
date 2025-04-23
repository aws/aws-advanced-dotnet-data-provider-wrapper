using NHibernate.Cfg.Loquacious;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NHibernate.Driver.AwsWrapper
{
    public static class AwsWrapperDriverConnectionConfigurationExtension
    {
        public static void UseAwsWrapperDriver<TDriver>(this IDbIntegrationConfigurationProperties cfg) where TDriver : IDriver => cfg.Driver<AwsWrapperDriver<TDriver>>();
    }
}
