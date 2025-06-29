using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AgOpenGPS.Core.Communication
{
    /// <summary>
    /// Implements the MQTT Section Control Rule for AgOpenGPS.
    /// Responsible for subscribing to section control commands and
    /// publishing status, acknowledgements and errors.
    /// </summary>
    public class MqttSectionControlClient : IDisposable
    {
        private readonly IMqttClient _client;
        private readonly IMqttClientOptions _options;
        private readonly Timer _heartbeatTimer;

        public event Action<int, bool>? SectionCommandReceived;
        public event Action? ShutdownRequested;

        public MqttSectionControlClient(string host, int port = 1883)
        {
            var factory = new MqttFactory();
            _client = factory.CreateMqttClient();
            _options = new MqttClientOptionsBuilder()
                .WithTcpServer(host, port)
                .Build();
            _heartbeatTimer = new Timer(async _ => await PublishStatusAsync(), null, Timeout.Infinite, Timeout.Infinite);
            _client.ApplicationMessageReceivedAsync += HandleMessageAsync;
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            await _client.ConnectAsync(_options, cancellationToken);
            await _client.SubscribeAsync("agopen/sectioncontrol/commands", cancellationToken);
            await _client.SubscribeAsync("agopen/system/commands", cancellationToken);
            _heartbeatTimer.Change(TimeSpan.Zero, TimeSpan.FromSeconds(30));
        }

        private async Task HandleMessageAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            var payload = Encoding.UTF8.GetString(arg.ApplicationMessage.Payload ?? Array.Empty<byte>());
            try
            {
                using var doc = JsonDocument.Parse(payload);
                var root = doc.RootElement;
                if (root.TryGetProperty("command", out var cmdProp))
                {
                    var command = cmdProp.GetString();
                    switch (command)
                    {
                        case "set_section":
                            int section = root.GetProperty("section").GetInt32();
                            string state = root.GetProperty("state").GetString() ?? "off";
                            SectionCommandReceived?.Invoke(section, state.Equals("on", StringComparison.OrdinalIgnoreCase));
                            await PublishAckAsync(section, state);
                            break;
                        case "shutdown":
                            ShutdownRequested?.Invoke();
                            await PublishAckAsync("shutdown");
                            break;
                        default:
                            await PublishErrorAsync("unknown_command", command);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                await PublishErrorAsync("invalid_payload", ex.Message);
            }
        }

        public async Task PublishStatusAsync()
        {
            var payload = JsonSerializer.Serialize(new
            {
                status = "online",
                program = "AgOpenGPS",
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            });
            var message = new MqttApplicationMessageBuilder()
                .WithTopic("agopen/sectioncontrol/status")
                .WithPayload(payload)
                .WithAtLeastOnceQoS()
                .Build();
            if (_client.IsConnected)
            {
                await _client.PublishAsync(message);
            }
        }

        private async Task PublishAckAsync(int section, string state)
        {
            var payload = JsonSerializer.Serialize(new
            {
                ack = "set_section",
                section,
                state
            });
            var message = new MqttApplicationMessageBuilder()
                .WithTopic("agopen/sectioncontrol/status")
                .WithPayload(payload)
                .WithAtLeastOnceQoS()
                .Build();
            if (_client.IsConnected)
            {
                await _client.PublishAsync(message);
            }
        }

        private async Task PublishAckAsync(string command)
        {
            var payload = JsonSerializer.Serialize(new { ack = command });
            var message = new MqttApplicationMessageBuilder()
                .WithTopic("agopen/sectioncontrol/status")
                .WithPayload(payload)
                .WithAtLeastOnceQoS()
                .Build();
            if (_client.IsConnected)
            {
                await _client.PublishAsync(message);
            }
        }

        private async Task PublishErrorAsync(string error, string? detail = null)
        {
            var payload = JsonSerializer.Serialize(new { error, detail });
            var message = new MqttApplicationMessageBuilder()
                .WithTopic("agopen/sectioncontrol/status")
                .WithPayload(payload)
                .WithAtLeastOnceQoS()
                .Build();
            if (_client.IsConnected)
            {
                await _client.PublishAsync(message);
            }
        }

        public void Dispose()
        {
            _heartbeatTimer.Dispose();
            _client?.Dispose();
        }
    }
}
