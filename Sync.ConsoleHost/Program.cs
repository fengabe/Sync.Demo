using System;
using System.Collections.Generic;
using DXP.Adam.Recovery.XPublish;

namespace Sync.ConsoleHost {
    class Program {
        static void Main(string[] args) {
            var coordinator = new RestoreCoordinator();

            //var templateIds = new List<Guid>
            //                  {
            //                      new Guid("DFEC607E-D6AD-4BAE-A335-00F26F6D57B8"),
            //                      new Guid("B2A5F5EA-015A-46D7-BE0F-03B5C305314E")
            //                  };

            //DateTime time = DateTime.Now;
            //coordinator.RestoreTemplates(templateIds);

            var masterIds = new List<Guid>
                              {
                                  new Guid("27DF0B0A-565A-4BAE-B931-5CDD7CC04A49"),
                                  new Guid("3EF54F90-85B2-4BC6-86D9-944FC9CDCD50")
                              };

            DateTime time = DateTime.Now;
            coordinator.RestoreMasters(masterIds);
           
            var miliseconds = DateTime.Now.Subtract(time).TotalMilliseconds;
            Console.WriteLine("Restore time: {0}", miliseconds);
            Console.WriteLine("Press ENTER to end...");
            Console.ReadKey();
            Console.WriteLine("Finished");
        }
    }
}
