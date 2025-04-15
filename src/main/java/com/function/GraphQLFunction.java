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

    public static final String GRAPHQL_PATH = "/roles";

    private static final List<Rol> rolList = new ArrayList<>();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final GraphQL graphQL;
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASSWORD");
    private static final String dbAlias = System.getenv("DB_ALIAS");
    private static final String walletPath = System.getenv("WALLET_PATH");

    static {
        // Definir esquema GraphQL
        GraphQLObjectType rolType = GraphQLObjectType.newObject()
            .name("Rol")
            .field(f -> f.name("id").type(Scalars.GraphQLInt))
            .field(f -> f.name("name").type(Scalars.GraphQLString))
            .build();

        // Query
        GraphQLObjectType queryType = GraphQLObjectType.newObject()
            .name("Query")
            .field(f -> f.name("getRolById")
                .type(rolType)
                .argument(a -> a.name("id").type(Scalars.GraphQLInt))
                .dataFetcher(env -> {
                    Integer id = env.getArgument("id");
                    try {
                        return getRowById(id);
                    } catch (Exception e) {
                        throw new RuntimeException("Error al obtener rol por id: " + e.getMessage());
                    }
                }))
            .field(f -> f.name("getAllRoles")
                .type(GraphQLList.list(rolType))
                .dataFetcher(env -> {
                    try {
                        return getAllRoles();
                    } catch (Exception e) {
                        throw new RuntimeException("Error al obtener todos los roles: " + e.getMessage());
                    }
                }))
            .build();

        // Mutation
        GraphQLObjectType mutationType = GraphQLObjectType.newObject()
            .name("Mutation")
            .field(f -> f.name("createRol")
                .type(rolType)
                .argument(a -> a.name("id").type(Scalars.GraphQLInt))
                .argument(a -> a.name("name").type(Scalars.GraphQLString))
                .dataFetcher(env -> {
                    Rol rol = new Rol(
                        env.getArgument("id"),
                        env.getArgument("name"));
                    try {
                        createRow(rol);
                    } catch (Exception e) {
                        throw new RuntimeException("Error al crear rol: " + e.getMessage());
                    }
                    return rol;
                }))
            .field(f -> f.name("updateRol")
                .type(rolType)
                .argument(a -> a.name("id").type(Scalars.GraphQLInt))
                .argument(a -> a.name("name").type(Scalars.GraphQLString))
                .dataFetcher(env -> {
                    Integer id = env.getArgument("id");
                    String name = env.getArgument("name");
                    try {
                        return updateRow(id, name);
                    } catch (Exception e) {
                        throw new RuntimeException("Error al actualizar rol: " + e.getMessage());
                    }
                }))
            .field(f -> f.name("deleteRol")
                .type(Scalars.GraphQLBoolean)
                .argument(a -> a.name("id").type(Scalars.GraphQLInt))
                .dataFetcher(env -> {
                    Integer id = env.getArgument("id");
                    try {
                        deleteRow(id);
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException("Error al eliminar rol: " + e.getMessage());
                    }
                }))
            .build();

        GraphQLSchema schema = GraphQLSchema.newSchema()
            .query(queryType)
            .mutation(mutationType)
            .build();

        graphQL = GraphQL.newGraphQL(schema).build();
    }

    @FunctionName("roles")
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

    private static Rol getRowById(Integer id) throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT id, name FROM rols WHERE id = ?")) {
                stmt.setLong(1, id);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return new Rol(rs.getInt("id"), rs.getString("name"));
                    }
                }
            }
        }
        return null;
    }

    private static List<Rol> getAllRoles() throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        List<Rol> roles = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT id, name FROM rols")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    System.out.println("entro al executeQuery" );
                    while (rs.next()) {
                        System.out.println("entro al next" );
                        roles.add(new Rol(rs.getInt("id"), rs.getString("name")));
                    }
                } catch(Exception e) {
                    System.out.println("Roles: " + e.getMessage());
                }
            } catch(Exception e) {
                System.out.println("Roles: " + e.getMessage());
            }
        } catch(Exception e) {
            System.out.println("Roles: " + e.getMessage());
        }
        return roles;
    }

    private static void createRow(Rol rol) throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO rols(id, name) VALUES (?, ?)")) {
                stmt.setLong(1, rol.getId());
                stmt.setString(2, rol.getName());
                stmt.executeUpdate();
            }
        }
    }

    private static Rol updateRow(Integer id, String name) throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("UPDATE rols SET name = ? WHERE id = ?")) {
                stmt.setString(1, name);
                stmt.setLong(2, id);
                stmt.executeUpdate();
            }
        }
        return getRowById(id);
    }

    private static void deleteRow(Integer id) throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@" + dbAlias + "?TNS_ADMIN=" + walletPath;

        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPass);
        props.put("oracle.net.ssl_server_dn_match", "true");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM rols WHERE id = ?")) {
                stmt.setLong(1, id);
                stmt.executeUpdate();
            }
        }
    }
}
