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
import java.util.HashMap;
import java.util.Random;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;


@Service
public class UrlService {

    @Autowired
    private CassandraConn cassandraConn;

    @Value("${cassandra.keyspace}")
    private String keyspaceName;


    //HashMap to store the longUrl and the randomly generated string
    HashMap<String,String> urlMap = new HashMap<>();

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

    public String getUrlById(String longUrl) throws Exception {
        if (urlMap.containsKey(longUrl)) {
            return urlMap.get(longUrl);
        }
        Result<Url> result = null;
        Statement statement = QueryBuilder
                .select()
                .from(keyspaceName, "urldata")
                .where(eq("id", longUrl)).setFetchSize(10);
        statement.setConsistencyLevel(cassandraConn.getConsistencyLevel());
        try {
            ResultSet resultSet = session.execute(statement);
            result = mapper.map(resultSet);

        } catch (Exception e) {
            throw new Exception("Failed to search Url for urlId :" + longUrl.toString(), e);
        }
        if (result != null) {
            String sUrl = result.one().getUrl();
            urlMap.put(longUrl, sUrl);
            return sUrl;
        }
        return null;
    }

    public String createUrl(String longUrl) throws Exception {
        String sUrl = getUrlById(longUrl);
        if (sUrl != null) {
            return sUrl;
        }
        sUrl = toShortUrl(longUrl);
        urlMap.put(longUrl, sUrl);
        return createUrl(longUrl, sUrl);
    }

    private String createUrl(String id, String url) throws Exception {
        Url urlDto = new Url(id, url);
        mapper.save(urlDto);
        return urlDto.getUrl();
    }

    public void delete(String urlId) throws Exception {
        String url = getUrlById(urlId);
        mapper.delete(new Url(urlId, url));
    }

    // Encodes a URL to a shortened URL.
    private String toShortUrl(String longUrl) {
        Random rand = new Random();
        int urlLen = 6;
        char [] shortURL = new char[urlLen];
        String randChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

        for(int i = 0; i < urlLen; i++ )
            shortURL[i] = randChars.charAt(rand.nextInt(randChars.length()));
        return "http://tinyurl.com/"+ new String(shortURL);
    }
}