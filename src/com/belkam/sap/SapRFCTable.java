package com.belkam.sap;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.DataProviderException;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;


public class SapRFCTable
{
    static class MyDestinationDataProvider implements DestinationDataProvider
    {
        private DestinationDataEventListener eL;
        private HashMap<String, Properties> secureDBStorage = new HashMap<String, Properties>();

        public Properties getDestinationProperties(String destinationName)
        {
            try
            {
                //read the destination from DB
                Properties p = secureDBStorage.get(destinationName);

                if(p!=null)
                {
                    //check if all is correct, for example
                    if(p.isEmpty())
                        throw new DataProviderException(DataProviderException.Reason.INVALID_CONFIGURATION, "destination configuration is incorrect", null);

                    return p;
                }

                return null;
            }
            catch(RuntimeException re)
            {
                throw new DataProviderException(DataProviderException.Reason.INTERNAL_ERROR, re);
            }
        }

        //An implementation supporting events has to retain the eventListener instance provided
        //by the JCo runtime. This listener instance shall be used to notify the JCo runtime
        //about all changes in destination configurations.
        public void setDestinationDataEventListener(DestinationDataEventListener eventListener)
        {
            this.eL = eventListener;
        }

        public boolean supportsEvents()
        {
            return true;
        }

              void changeProperties(String destName, Properties properties)
        {
            synchronized(secureDBStorage)
            {
                if(properties==null)
                {
                    if(secureDBStorage.remove(destName)!=null)
                        eL.deleted(destName);
                }
                else
                {
                    secureDBStorage.put(destName, properties);
                    eL.updated(destName); // create or updated
                }
            }
        }
    } // end of MyDestinationDataProvider

    //business logic
    void executeCalls(String destName, Properties prp)
        {
        JCoDestination dest;
        Writer writer = null;
        try
        {
            dest = JCoDestinationManager.getDestination(destName);

            JCoFunction function = dest.getRepository().getFunction(prp.getProperty("fnc_name"));
            if(function == null)
                throw new RuntimeException(prp.getProperty("fnc_name") + " not found in SAP.");
            try
            {
                function.execute(dest);
            }
            catch(AbapException e)
            {
                System.out.println(e.toString());
                return;
            }

            JCoTable users =  function.getTableParameterList().getTable(0);

            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(prp.getProperty("file_name")), prp.getProperty("file_encoding")));
            for (int i = 0; i < users.getNumRows(); i++)
            {
                users.setRow(i);
                StringBuilder sBuilder = new StringBuilder();
                for (int j = 0; j < users.getNumColumns(); j++) {
                    sBuilder.append(users.getString(j));
                    sBuilder.append(prp.getProperty("delim_col"));
                }
                sBuilder.append("\n");
                writer.write(sBuilder.toString());
            }


        }
        catch(JCoException e)
        {
            e.printStackTrace();
            System.out.println("Execution on destination " + destName+ " failed");
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    static Properties getDestinationPropertiesFromUI()
    {
        //adapt parameters in order to configure a valid destination
        Properties connectProperties = new Properties();

        try {

            connectProperties.load(SapRFCTable.class.getResourceAsStream("sap.properties"));
        } catch (Exception e) {
            System.out.println("Error read property file: " + "com/belkam/sap/sap.properties");
            e.printStackTrace();
        }
        return connectProperties;
    }

    public static void main(String[] args)
    {

        MyDestinationDataProvider myProvider = new MyDestinationDataProvider();
        //register the provider with the JCo environment;
        //catch IllegalStateException if an instance is already registered
        try
        {
            com.sap.conn.jco.ext.Environment.registerDestinationDataProvider(myProvider);
        }
        catch(IllegalStateException providerAlreadyRegisteredException)
        {
            //somebody else registered its implementation,
            //stop the execution
            throw new Error(providerAlreadyRegisteredException);
        }

        String destName = "ABAP_AS";
        SapRFCTable test = new SapRFCTable();

        //set properties for the destination and ...
        Properties prp = getDestinationPropertiesFromUI();
        myProvider.changeProperties(destName, prp);
        //... work with it


        test.executeCalls(destName, prp);

    }

}
