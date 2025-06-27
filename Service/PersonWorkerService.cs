using System.Text.Json;
using k8sFormWorkerApp.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using StackExchange.Redis;

namespace k8sFormWorkerApp.Service
{
    public class PersonWorkerService : BackgroundService
    {
        private readonly ILogger<PersonWorkerService> _logger;
        private readonly IConnectionMultiplexer _redis;
        private readonly string _postgresConnectionString;
        private readonly IDatabase _database;

        public PersonWorkerService(ILogger<PersonWorkerService> logger, IConnectionMultiplexer redis, IConfiguration configuration)
        {
            _logger = logger;
            _redis = redis;
            _database = _redis.GetDatabase();
            _postgresConnectionString = configuration.GetConnectionString("Postgres") ?? throw new ArgumentNullException("Postgres connection string is required.");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("PersonWorkerService is started at: {time}", DateTimeOffset.Now);

            // Ensure PostgreSQL table exists
            await EnsureTableExistsAsync();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Pop from Redis queue (blocking operation with timeout)
                    var result = await _database.ListRightPopAsync("persons_queue");

                    if (result.HasValue)
                    {
                        var personJson = result.ToString();
                        _logger.LogInformation("Processing person data: {data}", personJson);

                        var personData = JsonSerializer.Deserialize<Person>(personJson);
                        if (personData != null)
                        {
                            await SaveToPostgresAsync(personData);
                            _logger.LogInformation("Successfully saved person: {firstName} {lastName}",
                                personData.FirstName, personData.LastName);
                        }
                    }
                    else
                    {
                        // No data available, wait before checking again
                        await Task.Delay(1000, stoppingToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing person data");
                    await Task.Delay(5000, stoppingToken); // Wait longer on error
                }
            }
            _logger.LogInformation("PersonWorkerService is stopping.");
        }


        private async Task EnsureTableExistsAsync()
        {
            try
            {
                using var connection = new NpgsqlConnection(_postgresConnectionString);
                await connection.OpenAsync();

                var createTableQuery = @"
                    CREATE TABLE IF NOT EXISTS persons (
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100) NOT NULL,
                        last_name VARCHAR(100) NOT NULL,
                        address TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )";

                using var command = new NpgsqlCommand(createTableQuery, connection);
                await command.ExecuteNonQueryAsync();

                _logger.LogInformation("PostgreSQL table 'persons' ensured to exist");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error ensuring PostgreSQL table exists");
                throw;
            }
        }

        private async Task SaveToPostgresAsync(Person person)
        {
            using var connection = new NpgsqlConnection(_postgresConnectionString);
            await connection.OpenAsync();

            var insertQuery = @"
                INSERT INTO persons (first_name, last_name, address, created_at) 
                VALUES (@firstName, @lastName, @address, @createdAt)";

            using var command = new NpgsqlCommand(insertQuery, connection);
            command.Parameters.AddWithValue("@firstName", person.FirstName);
            command.Parameters.AddWithValue("@lastName", person.LastName);
            command.Parameters.AddWithValue("@address", person.Address);
            command.Parameters.AddWithValue("@createdAt", person.CreatedAt);

            await command.ExecuteNonQueryAsync();
        }

    }
}
