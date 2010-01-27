/*
Copyright 2010 Computer and Automation Research Institute, Hungarian Academy of Sciences (SZTAKI)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package hu.sztaki.ilab.giraffe.core.factories;

import hu.sztaki.ilab.giraffe.core.conversion.ConversionManager;
import hu.sztaki.ilab.giraffe.core.util.Pair;
import hu.sztaki.ilab.giraffe.schema.datatypes.Parameters;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * For events and tasks a number of object must be instantiated dynamically.
 * ObjectInstantiator takes care of this. Class names and optional parameters
 * are registered with the addObject and generateCallingCode methods.
 * ObjectInstantiator will instantiate the objects it knows about with
 * instantiate().
 * @author neumark
 */
public class ObjectInstantiator {

    private String instancePrefix = "";
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ObjectInstantiator.class);
    // add an object to be instantiated.
    java.util.Map<String, hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance> toBeInstantiated =
            new java.util.HashMap<String, hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance>();
    // instantiated objects.
    java.util.Map<String, Pair<Class, Object>> instantiatedObjects = new java.util.HashMap<String, Pair<Class, Object>>();

    public ObjectInstantiator(String instancePrefix) {
        this.instancePrefix = instancePrefix;
    }

    // return true if no objects need to be instantiated.
    public boolean isEmpty() {return toBeInstantiated.isEmpty();}

    public Object get(String instanceName) {
        return instantiatedObjects.get(instanceName).second;
    }

    public String generateFieldDeclarations() {
        StringWriter codeWriter = new StringWriter();
        for (java.util.Map.Entry<String, hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance> o : toBeInstantiated.entrySet()) {
            String type = o.getValue().getClazz().replace('$', '.');
            codeWriter.write("  " + type + " " + o.getKey() + ";\n");
        }
        return codeWriter.toString();
    }

    public String generateGetInstancesCode(String refToThis) {
        StringWriter codeWriter = new StringWriter();
        for (java.util.Map.Entry<String, hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance> o : toBeInstantiated.entrySet()) {
           String type = o.getValue().getClazz().replace('$', '.');
            codeWriter.write("  " + o.getKey() + " = (" + type + ")" + refToThis + ".get(\"" + o.getKey() + "\");\n");
        }
        return codeWriter.toString();
    }

    public String generateCallingCode(hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance call, String input) {
        if (call.getClazz() != null) {
            String instanceName = this.instancePrefix + toBeInstantiated.size();
            toBeInstantiated.put(instanceName, call);
            String methodName = call.getMethod();
            if (methodName == null) {
                methodName = ProcessingNetworkGenerator.runTaskFunction;
            }
            return instanceName + "." + methodName + "(" + input + ")";
        }

        // calls to static classes require no action on our part.
        return call.getMethod() + "(" + input + ")";

    }

    public boolean instantiate() {
        for (java.util.Map.Entry<String, hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance> o : toBeInstantiated.entrySet()) {
            try {
                Object newInstance = null;
                Class c = ObjectInstantiator.class.getClassLoader().loadClass(o.getValue().getClazz());
                Constructor constructor = null;
                if (o.getValue().getParameters() != null) {
                    try {
                    constructor = c.getConstructor(new Class[] {Parameters.class});
                    newInstance = constructor.newInstance(o.getValue().getParameters());
                    } catch (NoSuchMethodException noConstructorEx) {
                        logger.error("An instance of class '"+o.getValue().getClazz()+"' (named '"+o.getKey()+"') is assigned a parameter, however no constructor accepting parameters were found in the class.\nTo fix this issue, add a constructor to the class which takes a parameter of type import hu.sztaki.ilab.giraffe.schema.datatypes.Parameters");
                        return false;
                    }
                } else {
                    // The class was not passed any constructors.
                    try {
                    constructor = c.getConstructor(new Class[] {});
                    newInstance = constructor.newInstance();
                    } catch (NoSuchMethodException noConstructorEx) {
                        logger.error("An instance of class '"+o.getValue().getClazz()+"' (named '"+o.getKey()+"') is assigned no parameters, however no default constructor could be found.");
                        return false;
                    }
                }
                try {
                    Method initMethod = c.getMethod("init",new Class[] {});
                    if ("boolean".equals(ConversionManager.getType(initMethod.getReturnType())) &&
                            Modifier.isPublic(initMethod.getModifiers()) &&
                            !Modifier.isStatic(initMethod.getModifiers())) {
                        java.lang.Boolean ret = (java.lang.Boolean)initMethod.invoke(newInstance, new java.lang.Object[] {});
                        if (!ret.booleanValue()) {
                            logger.error("Variable '"+o.getKey()+"' could not be initialized, because init() returned false.");
                            return false;
                        }
                        logger.debug("Variable '"+o.getKey()+"' initialized successfully.");
                    }
                } catch (NoSuchMethodException noConstructorEx) {
                    logger.debug("Initialization method "+o.getKey()+".init() will not be called because "+o.getValue().getClazz()+" has no public non-static method boolean init().");
                }
                instantiatedObjects.put(o.getKey(), new Pair<Class, Object>(c, newInstance));
                     
            } catch (Exception ex) {
                logger.error("Failed to instantiate class " + o.getValue().getClazz(), ex);
                return false;
            }
        }
        return true;
    }
}
