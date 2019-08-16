package com.test.service;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.test.model.Url;
import com.test.repository.CassandraConn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;


@Service
public class UrlService {

    @Autowired
    private CassandraConn cassandraConn;

    @Value("${cassandra.keyspace}")
    private String keyspaceName;

    private Session session;
    private MappingManager manager;
    private Mapper<Url> mapper;

    @PostConstruct
    public void init() {
        try {
            session = cassandraConn.getSession();
            manager = new MappingManager(session);
            mapper = manager.mapper(Url.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initiate UrlService", e);
        }
    }

    public Result<Url> getUrlById(String urlId) throws Exception {
        Result<Url> result = null;
        Statement statement = QueryBuilder
                .select()
                .from(keyspaceName, "urldata")
                .where(eq("id", urlId)).setFetchSize(10);
        statement.setConsistencyLevel(cassandraConn.getConsistencyLevel());
        try {
            ResultSet resultSet = session.execute(statement);
            result = mapper.map(resultSet);

        } catch (Exception e) {
            throw new Exception("Failed to search Url for urlId :" + urlId.toString(), e);
        }
        return result;
    }

    public String createUrl(String longUrl) throws Exception {
        String sUrl = "";
        return createUrl(sUrl, longUrl);
    }

    public String createUrl(String id, String url) throws Exception {
        Url urlDto = new Url(id, url);
        mapper.save(urlDto);
        return urlDto.getUrl();
    }

    public void delete(String urlId) throws Exception {
        Url url = getUrlById(urlId).one();
        mapper.delete(url);
    }
}