package dk.statsbiblioteket.doms.ecm.fedoravalidatorhook;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fcrepo.common.Constants;
import org.fcrepo.server.Context;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ObjectValidityException;
import org.fcrepo.server.errors.ServerInitializationException;
import org.fcrepo.server.management.Management;
import org.fcrepo.server.management.ManagementModule;
import org.fcrepo.server.proxy.AbstractInvocationHandler;
import org.fcrepo.server.storage.types.Validation;
import org.fcrepo.utilities.DateUtility;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.fcrepo.server.utilities.StreamUtility.enc;


/**
 * This invocationhandler is simple. If hooks the modifyObject method. When the state is set to Active, the validator
 * is invoked. A server exception with the problems is thrown, and the state is not changed.
 */
public class FedoraModifyObjectHook extends AbstractInvocationHandler {

    /** Logger for this class. */
    private static Log LOG = LogFactory.getLog(FedoraModifyObjectHook.class);


    public static final String HOOKEDMETHOD = "modifyObject";


    private boolean initialised = false;

    private Management management;
    private Server serverModule;

    public synchronized void init()  {
        if (initialised){
            return;
        }


        try {
            serverModule = Server.getInstance(new File(Constants.FEDORA_HOME), false);
        } catch (ServerInitializationException e) {
            LOG.error("Unable to get access to the server instance, the " +
                      "Validator will not be started.",e);
            return;
        } catch (ModuleInitializationException e) {
            LOG.error("Unable to get access to the server instance, the " +
                      "Validator will not be started.",e);
            return;
        }
        ManagementModule m_manager = getManagement();
        if (m_manager == null) {
            LOG.error("Unable to get Management module from server, the " +
                      "Validator will not start up.");
            return;
        }

        management = m_manager;
        initialised = true;

    }

    /**
     * Utility method to get the Management Module from the Server module
     *
     * @return the Management module
     */
    private ManagementModule getManagement() {
        ManagementModule module = (ManagementModule) serverModule.getModule(
                "org.fcrepo.server.management.Management");
        if (module == null) {
            module = (ManagementModule) serverModule.getModule(
                    "fedora.server.management.Management");
        }
        return module;

    }


    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable, IOException {

        LOG.debug("Entering method invoke in FedoraModifyObjectHook with arguments: method='"
                  +method.getName()+"' and arguments: " + Arrays.toString(args));
        if (!initialised){
            init();
        }

        if (!HOOKEDMETHOD.equals(method.getName())){
            return callMethod(method,args);
        }

        //If we are here, we have modifyObject
        LOG.info("We are hooking method "+method.getName());

        //args[0] = context
        //args[1] = pid
        //args[2] = state

        //If the call does not change the state to active, pass through
        String state = (String) args[2];
        if (!(state != null && state.startsWith("A"))){
            return callMethod(method,args);
        }


        String pid = args[1].toString();
        LOG.info("The method was called with the pid " + pid);
        Context context = (Context) args[0];

        Validation result = management.validate(context, pid, null);

        //Element docelement = result.getDocumentElement();

        if (result.isValid()){
            return callMethod(method,args);
        } else {
            throw new ObjectValidityException("Cannot set object '"+pid+"' to Active due to validation",result);
        }

    }



    private Object callMethod(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target,args);
        } catch (InvocationTargetException e) {

            Throwable thr = e.getTargetException();
            LOG.debug("Rethrowing this exception, and loosing the stacktrace",thr);
            throw thr;

        }

    }


    public String objectValidationToXml(Validation validation) {
        StringBuilder buffer = new StringBuilder();
        String pid = validation.getPid();
        Date date = validation.getAsOfDateTime();
        String dateString = "";
        boolean valid = validation.isValid();
        if (date != null) {
            dateString = DateUtility.convertDateToString(date);
        }
        buffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        buffer.append("<validation "
                      + "pid=\"" + enc(pid) + "\" " +
                      "valid=\"" + valid + "\">\n");
        buffer.append("  <asOfDateTime>" + dateString + "</asOfDateTime>\n");
        buffer.append("  <contentModels>\n");
        for (String model : validation.getContentModels()) {
            buffer.append("    <model>");
            buffer.append(enc(model));
            buffer.append("</model>\n");
        }
        buffer.append("  </contentModels>\n");

        buffer.append("  <problems>\n");
        for (String problem : validation.getObjectProblems()) {
            buffer.append("    <problem>");
            buffer.append(problem);
            buffer.append("</problem>\n");
        }
        buffer.append("  </problems>\n");

        buffer.append("  <datastreamProblems>\n");
        Map<String, List<String>> dsprobs = validation.getDatastreamProblems();
        for (String ds : dsprobs.keySet()) {
            List<String> problems = dsprobs.get(ds);
            buffer.append("    <datastream");
            buffer.append(" datastreamID=\"");
            buffer.append(ds);
            buffer.append("\">\n");
            for (String problem : problems) {
                buffer.append("      <problem>");
                buffer.append(problem);
                buffer.append("</problem>\n");
            }
            buffer.append("    </datastream>");
        }
        buffer.append("  </datastreamProblems>\n");
        buffer.append("</validation>");
        return buffer.toString();
    }


}