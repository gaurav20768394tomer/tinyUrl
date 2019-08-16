package com.test.rest;


import com.datastax.driver.mapping.Result;
import com.test.model.Url;
import com.test.service.UrlService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/urls")
@Api(value = "API to perform CRUD operation in a Url database maintained in apache cassandra",
        description = "This API provides the capability to perform CRUD operation in a Url " +
                "database maintained in apache cassandra", produces = "application/json")
public class UrlApi {

    private static final Logger logger = LoggerFactory.getLogger(UrlApi.class);

    @Autowired
    private UrlService urlService;

    private static final String NO_RECORD = "Url not found for Url Id : ";

    @ApiOperation(value = "Search Url by urlId", produces = "application/json")
    @RequestMapping(value = "/{urlId}", method = RequestMethod.GET)
    public ResponseEntity<Object> searchUrlById(@ApiParam(name = "urlId",
            value = "the long",
            required = true) @PathVariable String urlId) {
        logger.debug("Searching for url with urlId :: {}", urlId);
        Result<Url> urlResult = null;
        ResponseEntity<Object> response = null;
        try {
            urlResult = urlService.getUrlById(urlId);
            if (urlResult == null) {
                response = new ResponseEntity<Object>(NO_RECORD + urlId, HttpStatus.OK);
            } else {
                response = new ResponseEntity<Object>(urlResult.one(), HttpStatus.OK);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return new ResponseEntity<Object>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


    @ApiOperation(value = "Create a new Url", consumes = "application/json")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "url", value = "long url",
                    required = true, dataType = "String", paramType = "query") })
    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<Object> createUrl(
            @RequestHeader(name = "url") String urlLong) {
        logger.debug("Creating Url with name :: {}", urlLong);
        ResponseEntity<Object> response = null;
        try {
            String urlId = urlService.createUrl(urlLong);
            response = new ResponseEntity<Object>("Url created successfully with Id :" +
                    urlId, HttpStatus.OK);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return new ResponseEntity<Object>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @ApiOperation(value = "Delete a Url Object from Database", consumes = "application/json")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "urlId", value = "UrlID to delete",
                    required = true, dataType = "String", paramType = "header")})
    @RequestMapping(method = RequestMethod.DELETE)
    public ResponseEntity<Object> delete(
            @RequestHeader(name = "urlId") String urlId) {
        logger.debug("Deleting Url with urlId :: {}", urlId);
        ResponseEntity<Object> response = null;
        try {
            urlService.delete(urlId);
            response = new ResponseEntity<Object>("Url deleted successfully with Id :" +
                    urlId, HttpStatus.OK);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return new ResponseEntity<Object>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }
}
