using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Plukit.ReliableEndpoint {
    class Program {
         public static int Main(string[] args) {
             TestA.Run();
             TestB.Run();
             TestC.Run();
             return 0;
         }
    }
}
