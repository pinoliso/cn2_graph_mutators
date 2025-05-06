package com.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.Scalars;
import graphql.schema.*;

import java.util.*;
import java.sql.ResultSet;

public class GraphQLFunction {

    public static final String GRAPHQL_PATH = "/users";

    private static final List<User> userList = new ArrayList<>();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final GraphQL graphQL;
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASSWORD");
    private static final String dbAlias = System.getenv("DB_ALIAS");
    private static final String walletPath = System.getenv("WALLET_PATH");

    static {
        // Definir esquema GraphQL
        GraphQLObjectType userType = GraphQLObjectType.newObject()
            .name("User")
            .field(f -> f.name("id").type(Scalars.GraphQLInt))
            .field(f -> f.name("username").type(Scalars.GraphQLString))
            .field(f -> f.name("password").type(Scalars.GraphQLString))
            .field(f -> f.name("name").type(Scalars.GraphQLString))
            .field(f -> f.name("rol").type(Scalars.GraphQLString))
            .build();

        // Query
        GraphQLObjectType queryType = GraphQLObjectType.newObject()
            .name("Query")
            .field(f -> f.name("getUserById")
                .type(userType)
                .argument(a -> a.name("id").type(Scalars.GraphQLInt))
                .dataFetcher(env -> {
                    Integer id = env.getArgument("id");
                    try {
                        return getUserById(id);
                    } catch (Exception e) {
                        throw new RuntimeException("Error al obtener usuario por id: " + e.getMessage());
                    }
                }))
            .field(f -> f.name("getAllUsers")
                .type(GraphQLList.list(userType))
                .dataFetcher(env -> {
                    try {
                        return getAllUsers();
                    } catch (Exception e) {
                        throw new RuntimeException("Error al obtener todos los usuarios: " + e.getMessage());
                    }
                }))
            .build();

        GraphQLSchema schema = GraphQLSchema.newSchema()
            .query(queryType)
            .build();

        graphQL = GraphQL.newGraphQL(schema).build();
    }

    @FunctionName("users")
    public HttpResponseMessage graphqlHandler(
        @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) 
        HttpRequestMessage<String> request, ExecutionContext context) {

        try {
            JsonNode body = mapper.readTree(request.getBody());
            String query = body.get("query").asText();
            JsonNode variables = body.get("variables");

            ExecutionInput input = ExecutionInput.newExecutionInput()
                .query(query)
                .variables(variables != null ? mapper.convertValue(variables, Map.class) : new HashMap<>())
                .build();

            Map<String, Object> result = graphQL.execute(input).toSpecification();

            return request.createResponseBuilder(HttpStatus.OK)
                          .header("Content-Type", "application/json")
                          .body(mapper.writeValueAsString(result))
                          .build();

        } catch (Exception e) {
            context.getLogger().severe("Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                          .body("Internal Error: " + e.getMessage())
                          .build();
        }
    }

    private static User getUserById(Integer id) throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT id, username, password, name, rol FROM users WHERE id = ?")) {
                stmt.setLong(1, id);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return new User(rs.getLong("id"), rs.getString("username"), rs.getString("password"), rs.getString("name"), rs.getString("rol"));
                    }
                }
            }
        }
        return null;
    }

    private static List<User> getAllUsers() throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        List<User> users = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT id, username, password, name, rol FROM users")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        users.add(new User(rs.getLong("id"), rs.getString("username"), rs.getString("password"), rs.getString("name"), rs.getString("rol")));
                    }
                } catch(Exception e) {
                    System.out.println("Users: " + e.getMessage());
                }
            } catch(Exception e) {
                System.out.println("Users: " + e.getMessage());
            }
        } catch(Exception e) {
            System.out.println("Users: " + e.getMessage());
        }
        return users;
    }
}
