using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AwsLese
{
    public interface IDebugObserver
    {
        void WriteDebug(string message);
    }
}
