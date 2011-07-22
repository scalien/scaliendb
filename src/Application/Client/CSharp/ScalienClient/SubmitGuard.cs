using System;

namespace Scalien
{
    public class SubmitGuard : IDisposable
    {
        Client client;

        public SubmitGuard(Client client)
        {
            this.client = client;
        }

        public void Dispose()
        {
            client.Submit();
        }
    }
}
