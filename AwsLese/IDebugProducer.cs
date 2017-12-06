using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsLese
{
    public interface IDebugProducer
    {
        void SetDebug(bool isDebug);
        void Register(IDebugObserver observer);
        void Unregister(IDebugObserver observer);
    }
}
