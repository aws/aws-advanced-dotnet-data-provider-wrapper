using MySqlConnector;
using NHibernate.Hql.Ast.ANTLR.Tree;
using Npgsql;
using System.Data;
using System.Data.Common;

namespace AwsWrapperDataProvider.Tests
{
    public class BasicConnectivityTests
    {
        [Fact]
        public void MysqlWrapperConnectionTest()
        {
            const string connectionString = "Server=global-ohio-mysql-instance-1.c12pgqavxipt.us-east-2.rds.amazonaws.com;User ID=admin;Password=my_password_2020;Initial Catalog=test;";
            const string query = "select @@aurora_server_id";

            using (AwsWrapperConnection<MySqlConnection> connection = new(connectionString))
            {
                AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
                command.CommandText = query;

                // Alternative syntax
                //IDbCommand command = connection.CreateCommand();
                //command.CommandText = query;

                // Alternative syntax
                //AwsWrapperCommand2 command = connection.CreateCommand();
                //command.CommandText = query;

                // Alternative syntax
                //AwsWrapperCommand2<MySqlCommand> command = new AwsWrapperCommand2<MySqlCommand>(query, connection);

                // Alternative syntax
                //AwsWrapperCommand2<MySqlCommand> command = new();
                //command.Connection = connection;
                //command.CommandText = query;

                try
                {
                    connection.Open();
                    IDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        [Fact]
        public void MysqlWrapperConnectionDynamicTest()
        {
            const string connectionString = "Server=global-ohio-mysql-instance-1.c12pgqavxipt.us-east-2.rds.amazonaws.com;User ID=admin;Password=my_password_2020;Initial Catalog=test;" +
                "targetConnectionType=MySqlConnector.MySqlConnection,MySqlConnector;" +
                "targetCommandType=MySqlConnector.MySqlCommand,MySqlConnector" +
                "targetParameterType=MySqlConnector.MySqlParameter,MySqlConnector";

            const string query = "select @@aurora_server_id";

            using (AwsWrapperConnection connection = new(connectionString))
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = query;

                try
                {
                    connection.Open();
                    IDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        [Fact]
        public void MysqlWrapperConnectionWithParametersTest()
        {
            const string connectionString = "Server=global-ohio-mysql-instance-1.c12pgqavxipt.us-east-2.rds.amazonaws.com;User ID=admin;Password=my_password_2020;Initial Catalog=test;";
            const string query = "select @var1";

            using (AwsWrapperConnection<MySqlConnection> connection = new(connectionString))
            {
                AwsWrapperCommand<MySqlCommand> command = connection.CreateCommand<MySqlCommand>();
                command.CommandText = query;

                DbParameter dbParameter = command.CreateParameter();
                dbParameter.ParameterName = "@var1";
                dbParameter.DbType = DbType.String;
                dbParameter.Value = "qwerty";
                command.Parameters.Add(dbParameter);

                try
                {
                    connection.Open();
                    IDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        [Fact]
        public void PgWrapperConnectionTest()
        {
            const string connectionString = "Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=postgres;";
            const string query = "select aurora_db_instance_identifier()";

            using (AwsWrapperConnection<NpgsqlConnection> connection = new(connectionString))
            {
                AwsWrapperCommand<NpgsqlCommand> command = connection.CreateCommand<NpgsqlCommand>();
                command.CommandText = query;

                try
                {
                    connection.Open();
                    IDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        [Fact]
        public void openPgWrapperConnectionDynamicTest()
        {
            const string connectionString = "Host=global-ohio-pg.cluster-c12pgqavxipt.us-east-2.rds.amazonaws.com;Username=pgadmin;Password=my_password_2020;Database=postgres;" +
                "targetConnectionType=Npgsql.NpgsqlConnection,Npgsql;" +
                "targetCommandType=Npgsql.NpgsqlCommand,Npgsql" +
                "targetParameterType=Npgsql.NpgsqlParameter,Npgsql";

            const string query = "select aurora_db_instance_identifier()";

            using (AwsWrapperConnection connection = new(connectionString))
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = query;

                try
                {
                    connection.Open();
                    IDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

    }
}