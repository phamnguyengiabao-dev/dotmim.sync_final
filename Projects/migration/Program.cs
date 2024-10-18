using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Microsoft.Data.SqlClient;

namespace Migration
{
    internal class Program
    {
        private static string serverConnectionString = $"Server=BAO;Database=AdventureWorks;Integrated Security=True;TrustServerCertificate=True;";
        private static string clientConnectionString = $"Server=BAO;Database=Client;Integrated Security=True;TrustServerCertificate=True;";

        private static async Task Main(string[] args)
        {
            /*

            Ý tưởng ở đây là xem 2 máy khách sẽ xử lý quá trình di chuyển như thế nào
            Chúng ta cần thêm một cột [CreatedDate] vào bảng [Address]
            Cột này có giá trị null mặc định để cho phép máy khách vẫn đồng bộ ngay cả khi
            chúng không được di chuyển

            Máy khách đầu tiên (Sql Server) sẽ di chuyển ngay lập tức
            Máy khách thứ hai (Sqlite) sẽ không di chuyển và sẽ ở lại lược đồ cũ mà không có cột mới

            */
            await MigrateClientsUsingMultiScopesAsync().ConfigureAwait(false);
        }

        private static async Task MigrateClientsUsingMultiScopesAsync()
        {
            // Tạo máy chủ Sync provider
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Tạo 2 máy khách. Máy khách đầu tiên sẽ di chuyển, máy khách thứ 2 sẽ không có cột mới
            var client1Provider = new SqlSyncProvider(clientConnectionString);
            var databaseName = $"{Path.GetRandomFileName().Replace(".", string.Empty).ToLowerInvariant()}.db";
            var client2Provider = new SqliteSyncProvider(databaseName);

            // Tạo Thiết lập chuẩn (chọn bảng bị ảnh hưởng)
            var setup = new SyncSetup("Address", "Customer", "CustomerAddress");

            // Tạo các tác nhân sẽ xử lý toàn bộ quy trình
            var agent1 = new SyncAgent(client1Provider, serverProvider);
            var agent2 = new SyncAgent(client2Provider, serverProvider);

            // Sử dụng mẫu Tiến trình để xử lý tiến trình trong quá trình đồng bộ hóa
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

            // Đồng bộ đầu tiên có điểm bắt đầu
            // Để tạo một ví dụ đầy đủ, chúng ta sẽ sử dụng tên phạm vi khác nhau (v0, v1)
            // v0 là cơ sở dữ liệu ban đầu
            // v1 sẽ chứa cột mới trong bảng Địa chỉ
            var s1 = await agent1.SynchronizeAsync("v0", setup, progress).ConfigureAwait(false);
            Console.WriteLine("Initial Sync on Sql Server Client 1");
            Console.WriteLine(s1);

            var s2 = await agent2.SynchronizeAsync("v0", setup, progress).ConfigureAwait(false);
            Console.WriteLine("Initial Sync on Sqlite Client 2");
            Console.WriteLine(s2);

            // -----------------------------------------------------------------
            // Di chuyển bảng bằng cách thêm một cột mới
            // -----------------------------------------------------------------

            // Thêm một cột mới có tên là CreatedDate vào bảng Address, trên máy chủ
            await Helper.AddNewColumnToAddressAsync(new SqlConnection(serverConnectionString)).ConfigureAwait(false);
            Console.WriteLine("Column added on server");

            // -----------------------------------------------------------------
            // Server side the adventureworks database
            // -----------------------------------------------------------------

            // Tạo thiết lập mới với cùng các bảng
            // Chúng ta sẽ cung cấp một phạm vi mới (v1)
            // Vì phạm vi này chưa tồn tại, nên nó sẽ buộc DMS làm mới lược đồ và
            // lấy cột mới
            var setupAddress = new SyncSetup("Address", "Customer", "CustomerAddress");

            // Tạo một bộ điều phối máy chủ được sử dụng để Hủy cung cấp và Chỉ cung cấp bảng Address
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            // Cung cấp lại mọi thứ cho phạm vi mới v1 này,
            // Phương thức cung cấp này sẽ lấy lược đồ địa chỉ từ cơ sở dữ liệu,
            // vì tên phạm vi mới chưa tồn tại
            // nên nó sẽ chứa tất cả các cột, bao gồm cả cột Địa chỉ mới được thêm vào
            await remoteOrchestrator.ProvisionAsync("v1", setupAddress).ConfigureAwait(false);
            Console.WriteLine("Server migration with new column CreatedDate done.");

            // Tại thời điểm này, cơ sở dữ liệu máy chủ có hai phạm vi:
            // v0: phạm vi đầu tiên với bảng Địa chỉ không có cột mới
            // v1: phạm vi thứ hai với bảng Địa chỉ có cột mới CreatedDate

            // Hãy xem cơ sở dữ liệu trong studio quản lý SQL và xem sự khác biệt trong quy trình được lưu trữ

            // Bây giờ hãy thêm một hàng trên máy chủ (với cột mới)
            var addressId = await Helper.InsertOneAddressWithNewColumnAsync(new SqlConnection(serverConnectionString)).ConfigureAwait(false);
            Console.WriteLine($"New address row added with pk {addressId}");

            // -----------------------------------------------------------------
            // SQlite Client sẽ vẫn ở trên lược đồ cũ (không có cột CreatedDate mới)
            // -----------------------------------------------------------------

            // Trước hết, chúng ta vẫn có thể đồng bộ cơ sở dữ liệu cục bộ mà không cần phải di chuyển máy khách
            // cho phép các máy khách cũ không có cột mới tiếp tục đồng bộ bình thường
            // các máy khách cũ này sẽ tiếp tục đồng bộ trên phạm vi v0
            var s3 = await agent2.SynchronizeAsync("v0", setup, progress: progress).ConfigureAwait(false);
            Console.WriteLine($"Sqlite not migrated, doing a sync on first scope v0:");
            Console.WriteLine(s3);

            // Nếu chúng ta lấy hàng từ máy khách, chúng ta sẽ chèn hàng mới vào máy chủ,
            // nhưng không có cột mới
            var client2row = await Helper.GetLastAddressRowAsync(client2Provider.CreateConnection(), addressId).ConfigureAwait(false);
            Console.WriteLine(client2row);

            // -----------------------------------------------------------------
            // Máy khách SQL Server sẽ thêm cột và sẽ đồng bộ hóa trên phạm vi mới (với cột CreatedDate mới)
            // -----------------------------------------------------------------

            // Bây giờ chúng ta sẽ nâng cấp máy khách 1

            // thêm cột vào máy khách
            await Helper.AddNewColumnToAddressAsync(new SqlConnection(clientConnectionString)).ConfigureAwait(false);
            Console.WriteLine("Sql Server client1 migration with new column CreatedDate done.");

            // Cung cấp cho máy khách phạm vi V1 mới
            // Lấy phạm vi từ máy chủ và áp dụng cục bộ
            var sScopeInfo = await agent1.RemoteOrchestrator.GetScopeInfoAsync("v1").ConfigureAwait(false);
            var v1cScopeInfo = await agent1.LocalOrchestrator.ProvisionAsync(sScopeInfo).ConfigureAwait(false);
            Console.WriteLine("Sql Server client1 Provision done.");

            // nếu bạn xem các thủ tục được lưu trữ trên cơ sở dữ liệu sql cục bộ của mình
            // bạn sẽ thấy rằng mình có hai phạm vi (v0 và v1)

            // PHẦN KHÓ KHĂN
            /*
            Phạm vi v1 là mới.
            Nếu chúng ta đồng bộ ngay bây giờ, vì v1 là mới, chúng ta sẽ đồng bộ tất cả các hàng từ đầu
            Điều chúng ta muốn là đồng bộ từ điểm cuối cùng chúng ta đồng bộ phạm vi v0 cũ
            Đó là lý do tại sao chúng ta đang che giấu thông tin siêu dữ liệu từ v0 vào v1
            */
            var v1cScopeInfoClient = await agent1.LocalOrchestrator.GetScopeInfoClientAsync("v1").ConfigureAwait(false);
            var v0cScopeInfoClient = await agent1.LocalOrchestrator.GetScopeInfoClientAsync("v0").ConfigureAwait(false);
            v1cScopeInfoClient.ShadowScope(v0cScopeInfoClient);
            await agent1.LocalOrchestrator.SaveScopeInfoClientAsync(v1cScopeInfoClient).ConfigureAwait(false);

            // Bây giờ hãy kiểm tra một đồng bộ mới, trên phạm vi v1 mới này
            var s4 = await agent1.SynchronizeAsync("v1", progress: progress).ConfigureAwait(false);
            Console.WriteLine($"Sql Server client1 migrated, doing a sync on second scope v1:");
            Console.WriteLine(s4);

            // Nếu chúng ta lấy hàng máy khách từ cơ sở dữ liệu máy khách, nó sẽ chứa giá trị
            var client1row = await Helper.GetLastAddressRowAsync(new SqlConnection(clientConnectionString), addressId).ConfigureAwait(false);

            Console.WriteLine(client1row);

            // TÙY CHỌN
            // -----------------------------------------------------------------

            // Trên máy khách mới này, đã di chuyển, chúng ta không còn cần phạm vi v0 nữa
            // chúng ta có thể hủy cung cấp nó
            await agent1.LocalOrchestrator.DeprovisionAsync("v0", SyncProvision.StoredProcedures).ConfigureAwait(false);
            Console.WriteLine($"Deprovision of old scope v0 done on Sql Server client1");

            // -----------------------------------------------------------------
            // Cuối cùng, Máy khách SQLite sẽ di chuyển sang v1
            // -----------------------------------------------------------------

            // Đã đến lúc di chuyển máy khách sqlite
            // Thêm cột vào máy khách SQLite
            await Helper.AddNewColumnToAddressAsync(client2Provider.CreateConnection()).ConfigureAwait(false);
            Console.WriteLine($"Column eventually added to Sqlite client2");

            // Cung cấp máy khách SQLite với phạm vi V1 mới
            var v1cScopeInfo2 = await agent2.LocalOrchestrator.ProvisionAsync(sScopeInfo).ConfigureAwait(false);
            Console.WriteLine($"Provision v1 done on SQLite client2");

            // ShadowScope phạm vi cũ sang phạm vi mới
            var v1cScopeInfoClient2 = await agent2.LocalOrchestrator.GetScopeInfoClientAsync("v1").ConfigureAwait(false);
            var v0cScopeInfoClient2 = await agent2.LocalOrchestrator.GetScopeInfoClientAsync("v0").ConfigureAwait(false);
            v1cScopeInfoClient2.ShadowScope(v0cScopeInfoClient2);
            await agent2.LocalOrchestrator.SaveScopeInfoClientAsync(v1cScopeInfoClient2).ConfigureAwait(false);

            // trước tiên hãy thử đồng bộ hóa
            // Bây giờ hãy thử đồng bộ hóa mới, trên phạm vi mới này v1
            // Rõ ràng là chúng ta không có bất cứ thứ gì từ máy chủ
            var s5 = await agent2.SynchronizeAsync("v1", progress: progress).ConfigureAwait(false);
            Console.WriteLine(s5);

            // Nếu chúng ta lấy hàng từ máy khách, chúng ta có cột mới, nhưng giá trị vẫn là null
            // vì hàng này đã được đồng bộ hóa trước khi di chuyển máy khách
            client2row = await Helper.GetLastAddressRowAsync(client2Provider.CreateConnection(), addressId).ConfigureAwait(false);
            Console.WriteLine(client2row);

            // Những gì chúng ta có thể làm ở đây là chỉ cần đồng bộ hóa với Renit
            var s6 = await agent2.SynchronizeAsync("v1", SyncType.ReinitializeWithUpload, progress: progress).ConfigureAwait(false);
            Console.WriteLine($"Making a full Reinitialize sync on SQLite client2");
            Console.WriteLine(s6);

            // Và bây giờ hàng đã đúng
            // Nếu chúng ta lấy hàng từ máy khách, chúng ta sẽ có cột mới, nhưng giá trị vẫn là null
            // vì hàng này đã được đồng bộ hóa trước khi di chuyển máy khách
            client2row = await Helper.GetLastAddressRowAsync(client2Provider.CreateConnection(), addressId).ConfigureAwait(false);
            Console.WriteLine(client2row);

            // Di chuyển xong
            Console.WriteLine("End");
        }
    }
}