package dk.statsbiblioteket.doms.ecm.repository.fedoraclient;

import dk.statsbiblioteket.doms.ecm.repository.FedoraConnector;
import dk.statsbiblioteket.doms.ecm.repository.FedoraUserToken;
import dk.statsbiblioteket.doms.ecm.repository.PidList;
import dk.statsbiblioteket.doms.ecm.repository.exceptions.*;
import dk.statsbiblioteket.doms.webservices.Base64;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.ClientResponse;

import javax.swing.text.html.parser.DocumentParser;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: Sep 24, 2010
 * Time: 10:39:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class FedoraRestClient implements FedoraConnector{
    private FedoraUserToken token;

    private static Client client = Client.create();
    private WebResource restApi;
    private DocumentBuilderFactory builder;


    public FedoraRestClient() {
        builder = DocumentBuilderFactory.newInstance();
        builder.setNamespaceAware(true);
    }

    public void initialise(FedoraUserToken token) {
        //To change body of implemented methods use File | Settings | File Templates.
        this.token = token;

        String location = token.getServerurl();
        restApi = client.resource(location + "/objects/");

    }

    public boolean exists(String pid) throws
                                      IllegalStateException,
                                      FedoraIllegalContentException,
                                      FedoraConnectionException,
                                      InvalidCredentialsException {
        try {
            restApi.path(pid)
                    .queryParam("format","text/xml")
                    .header("Authorization",credsAsBase64())
                    .get(String.class);
            return true;
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus()
                == ClientResponse.Status.UNAUTHORIZED.getStatusCode()) {
                throw new InvalidCredentialsException(
                        "Invalid Credentials Supplied",
                        e);
            } else if (e.getResponse().getStatus() == ClientResponse.Status.NOT_FOUND.getStatusCode()){
                return false;
            }
            else  {
                throw new FedoraIllegalContentException("Fedora failed",e);
            }

        }

    }

    protected String credsAsBase64(){
        String preBase64 = token.getUsername() + ":" + token.getPassword();
        String base64 = Base64.encodeBytes(preBase64.getBytes());
        return "Basic "+base64;
    }

    public boolean isDataObject(String pid) throws
                                            IllegalStateException,
                                            FedoraIllegalContentException,
                                            FedoraConnectionException,
                                            InvalidCredentialsException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isTemplate(String pid) throws
                                          IllegalStateException,
                                          ObjectNotFoundException,
                                          FedoraConnectionException,
                                          FedoraIllegalContentException,
                                          InvalidCredentialsException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isContentModel(String pid) throws
                                              IllegalStateException,
                                              FedoraIllegalContentException,
                                              FedoraConnectionException,
                                              InvalidCredentialsException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public PidList query(String query) throws
                                       IllegalStateException,
                                       FedoraConnectionException,
                                       FedoraIllegalContentException,
                                       InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean addRelation(String from, String relation, String to) throws
                                                                        IllegalStateException,
                                                                        ObjectNotFoundException,
                                                                        FedoraConnectionException,
                                                                        FedoraIllegalContentException,
                                                                        InvalidCredentialsException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean addLiteralRelation(String from,
                                      String relation,
                                      String value,
                                      String datatype) throws
                                                       IllegalStateException,
                                                       ObjectNotFoundException,
                                                       FedoraConnectionException,
                                                       FedoraIllegalContentException,
                                                       InvalidCredentialsException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Document getObjectXml(String pid) throws
                                             IllegalStateException,
                                             ObjectNotFoundException,
                                             FedoraConnectionException,
                                             FedoraIllegalContentException,
                                             InvalidCredentialsException {

        try {
            String object = restApi.path(pid)
                    .path("/objectXML")
                    .header("Authorization", credsAsBase64())
                    .get(String.class);
            return builder.newDocumentBuilder().parse(new ByteArrayInputStream(object.getBytes()));
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus()
                == ClientResponse.Status.UNAUTHORIZED.getStatusCode()) {
                throw new InvalidCredentialsException(
                        "Invalid Credentials Supplied",
                        e);
            } else if (e.getResponse().getStatus() == ClientResponse.Status.NOT_FOUND.getStatusCode()){
                throw new ObjectNotFoundException("Object not found",e);
            }
            else  {
                throw new FedoraIllegalContentException("Fedora failed",e);
            }

        } catch (IOException e) {
            throw new FedoraIllegalContentException("Failed to parse the returned xml",e);
        } catch (SAXException e) {
            throw new FedoraIllegalContentException("Failed to parse the returned xml",e);
        } catch (ParserConfigurationException e) {
            throw new FedoraIllegalContentException("Failed to parse the returned xml",e);
        }

    }

    public String ingestDocument(Document newobject, String logmessage) throws
                                                                        IllegalStateException,
                                                                        FedoraConnectionException,
                                                                        FedoraIllegalContentException,
                                                                        InvalidCredentialsException {
        try {
            String object = restApi.path(pid)
                    .path("/objectXML")
                    .header("Authorization", credsAsBase64())
                    .get(String.class);
            return builder.newDocumentBuilder().parse(new ByteArrayInputStream(object.getBytes()));
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus()
                == ClientResponse.Status.UNAUTHORIZED.getStatusCode()) {
                throw new InvalidCredentialsException(
                        "Invalid Credentials Supplied",
                        e);
            } else if (e.getResponse().getStatus() == ClientResponse.Status.NOT_FOUND.getStatusCode()){
                throw new ObjectNotFoundException("Object not found",e);
            }
            else  {
                throw new FedoraIllegalContentException("Fedora failed",e);
            }

        } catch (IOException e) {
            throw new FedoraIllegalContentException("Failed to parse the returned xml",e);
        } catch (SAXException e) {
            throw new FedoraIllegalContentException("Failed to parse the returned xml",e);
        } catch (ParserConfigurationException e) {
            throw new FedoraIllegalContentException("Failed to parse the returned xml",e);
        }

    }

    public List<Relation> getRelations(String pid) throws
                                                   IllegalStateException,
                                                   FedoraConnectionException,
                                                   ObjectNotFoundException,
                                                   FedoraIllegalContentException,
                                                   InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public List<Relation> getRelations(String pid,
                                       String relation) throws
                                                        IllegalStateException,
                                                        FedoraConnectionException,
                                                        ObjectNotFoundException,
                                                        FedoraIllegalContentException,
                                                        InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Document getDatastream(String pid, String dsid) throws
                                                           IllegalStateException,
                                                           DatastreamNotFoundException,
                                                           FedoraConnectionException,
                                                           FedoraIllegalContentException,
                                                           ObjectNotFoundException,
                                                           InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public PidList getContentModels(String pid) throws
                                                IllegalStateException,
                                                FedoraConnectionException,
                                                ObjectNotFoundException,
                                                FedoraIllegalContentException,
                                                InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public PidList getInheritingContentModels(String cmpid) throws
                                                            IllegalStateException,
                                                            FedoraConnectionException,
                                                            ObjectNotFoundException,
                                                            ObjectIsWrongTypeException,
                                                            FedoraIllegalContentException,
                                                            InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public List<String> listDatastreams(String pid) throws
                                                    IllegalStateException,
                                                    FedoraConnectionException,
                                                    ObjectNotFoundException,
                                                    FedoraIllegalContentException,
                                                    InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getUsername() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public PidList getInheritedContentModels(String cmpid) throws
                                                           FedoraConnectionException,
                                                           ObjectNotFoundException,
                                                           ObjectIsWrongTypeException,
                                                           FedoraIllegalContentException,
                                                           InvalidCredentialsException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean authenticate() throws FedoraConnectionException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getUser() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
