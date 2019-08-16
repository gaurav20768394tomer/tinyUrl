package com.test.model;


import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Objects;

@Table(keyspace = "tinyurl", name = "urldata")
public class Url {

    @PartitionKey
    @Column(name = "id")
    private String id;

    @ClusteringColumn
    @Column(name = "url")
    private String url;

    public Url(String id, String firstName) {
        this.id = id;
        this.url = firstName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Url person = (Url) o;
        return Objects.equals(id, person.id) && Objects.equals(url, person.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, url);
    }

    @Override
    public String toString() {
        return "Url{"
                + "id='"
                + id
                + '\''
                + ", url='"
                + url
                + '}';
    }
}