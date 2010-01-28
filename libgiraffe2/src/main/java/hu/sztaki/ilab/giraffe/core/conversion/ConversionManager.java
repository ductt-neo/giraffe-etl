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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.sztaki.ilab.giraffe.core.conversion;

import hu.sztaki.ilab.giraffe.core.factories.ObjectInstantiator;
import hu.sztaki.ilab.giraffe.core.util.StringUtils;
import hu.sztaki.ilab.giraffe.core.xml.annotation.Conversion;
import java.lang.reflect.*;
import hu.sztaki.ilab.giraffe.core.xml.ConfigurationManager;
import hu.sztaki.ilab.giraffe.schema.datatypes.ObjectInstance;
import hu.sztaki.ilab.giraffe.schema.datatypes.Parameters;
import hu.sztaki.ilab.giraffe.schema.process_definitions.ConversionHint;

/**
 * The ConversionManager class keeps track of which objects and methods can be used to convert one field type into
 * another. For stream input, the conversion is always String -> native Java type (eg. Integer or Date), while the
 * JDBC case is more complicated (JDBC object type -> java native type).
 * ConversionManager's responsibilities
 * 1. Keep track of all registered static conversion methods and conversion classes.
 * 2. Generate conversion code upon request for ProcessingNetworkGenerator.
 * 3. Use and ObjectInstantiator to create instances for used conversion classes.
 *
 * There are 3 ways in which a conversion may be performed:
 * 1. Calling a static method which has the appropriate input & output types. These methods are marked with
 * the @Conversion annotation.
 * 2. Calling the convert(inputs1, input2, ...) function of a conversion class. These classes are also marked with the
 * @Conversion annotation. Optional parameters can be passed to the conversion class using the setParameters() method.
 * 3. Calling a method of the object which is to be converted. These methods must be marked with the
 * @Conversion annotation as well. 
 * An optional name attribute can be assigned to the Conversion annotation, which allows the process definition to explicitly refer to
 * that conversion function. If there are several conversion functions to choose from, then the priority is:
 * 0. If the conversion function explicitly referred to by name, use it.
 * 1. Choose the object's own conversion function is such a function exists.
 * 2. If a conversion class exists, use that.
 * 3. Use the static conversion method.
 * If there are several viable conversion functions with the same priority, then the choice is left to giraffe (unless explicitly named).
 *
 * It is not possible to ask the java runtime for all classes or static methods with a given annotation, so the conversion
 * classes must be listed explicitly. This is performed using the exports mechanism:
 * @author neumark
 */
public class ConversionManager {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ConversionManager.class);
    private final ConfigurationManager configurationManager;
    // maps processing network (terminals type, terminal name, field name) triples to conversion hints.
    // eg: "datasource:logfile:timestamp" is mapped to a ConversionHint object.
    // If several hints are given for a triple, then the later hints override the newer ones.
    private final java.util.Map<String, ConversionHint> conversionHints = new java.util.HashMap<String, ConversionHint>();
    private final static ConversionFunction toString = new ConversionFunction();

    static {
        toString.methodName = "toString";
        toString.type = ConversionFunction.Type.INSTANCE_METHOD;
    }

    // A ConversionFunction describes where the code lives which performs the conversion. There are 3 possibilities, each corresponding to a single possible value of Type:
    static class ConversionFunction {

        enum Type {

            // The 3 types of conversion functions listed in order of preference.
            INSTANCE_METHOD, // The conversion method is a member of the input object, like toString()
            CONVERSION_CLASS, // The conversion method is not static, thus a conversion class must be instantiated for the conversion to take place.
            STATIC_METHOD // The conversion code is a static method.
        };
        // For all types:
        String name;
        String inputType;
        String outputType;
        Type type;
        // For all three:
        String methodName; // Only the method's name.
        String canonicalClassName; // The class's name, eg com.foo.bar.Outer.Inner
        String classloaderClassName; // eg. com.foo.bar.Outer$Inner
        // optional
        Parameters parameters;

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ConversionFunction) {
                return ((ConversionFunction) obj).hashCode() == this.hashCode();
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + (this.name != null ? this.name.hashCode() : 0);
            hash = 97 * hash + (this.inputType != null ? this.inputType.hashCode() : 0);
            hash = 97 * hash + (this.outputType != null ? this.outputType.hashCode() : 0);
            hash = 97 * hash + this.type.hashCode();
            hash = 97 * hash + (this.methodName != null ? this.methodName.hashCode() : 0);
            hash = 97 * hash + (this.canonicalClassName != null ? this.canonicalClassName.hashCode() : 0);
            return hash;
        }
    }
    // ConversionFunctionsByType maps input type -> output type -> conversion function
    // conversion functions for the same types are overridden by later conversion functions.
    private final java.util.Map<String, java.util.Map<String, ConversionFunction>> conversionFunctionsByType = new java.util.HashMap<String, java.util.Map<String, ConversionFunction>>();
    // ConversionFunctionsByName maps name -> conversion functions. If several functions have the same name, then the last one to be read wins.
    private final java.util.Map<String, ConversionFunction> conversionFunctionsByName = new java.util.HashMap<String, ConversionFunction>();

    public ConversionManager(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        for (Class currentClass : configurationManager.getExportReader().getExportedClasses()) {
            detectConversionFunctions(currentClass);
        }
    }

    private String getConversionCode(ConversionFunction cfn, String input, ObjectInstantiator objInst) {
        switch (cfn.type) {
            case STATIC_METHOD: // The conversion code is a static method.
                return cfn.canonicalClassName + "." + cfn.methodName + "(" + input + ")";
            case CONVERSION_CLASS: // The conversion method is not static, thus a conversion class must be instantiated for the conversion to take place.
                // make sure the converison class is instantiated.
                ObjectInstance inst = new ObjectInstance();
                inst.setClazz(cfn.classloaderClassName);
                inst.setMethod(cfn.methodName);
                inst.setParameters(cfn.parameters);
                return objInst.generateCallingCode(inst, input);
            case INSTANCE_METHOD: // The conversion method is a member of the input object, like toString()
                return "(("+input+"==null)?null:"+input + "." + cfn.methodName + "())";
        }
        return null;
    }

    private ConversionFunction getHint(String hintId) {
        if (!this.conversionHints.containsKey(hintId)) {
            return null;
        }
        ConversionHint hint = this.conversionHints.get(hintId);
        // A conversion hint has been defined for this field: use it.
        // First, we try to identify the conversion function.
        if (null != hint.getName()) {
            ConversionFunction cfn = this.conversionFunctionsByName.get(hint.getName());
            if (!this.conversionFunctionsByName.containsKey(hint.getName())) {
                logger.error("Conversion hint for " + hintId + " field " + hint.getField() + " refers to a conversion function " + hint.getName() + " but no such conversion function could be found.");
                return null;
            }
            cfn.parameters = hint.getParameters();
            return cfn;
        }
        if (null != hint.getConversionMethod()) {
            // conversionMethod should be the fully qualified method name of the converison method, eg:
            // hu.sztaki.ilab.giraffe.SomeClass.conversionMethod.
            String parts[] = hint.getConversionMethod().split("\\.");
            String className = "";
            String methodName = parts[parts.length - 1];
            for (int i = 0; i < (parts.length - 1); ++i) {
                if (className.length() > 0) {
                    className += ".";
                }
                className += parts[i];
            }
            ConversionFunction cfn = conversionFunctionFactory(className, methodName);
            cfn.parameters = hint.getParameters();
            return cfn;
        }
        logger.error("A conversion hint must refer to the conversion function by its name or method.");
        return null;
    }

    public String getConversionCode(
            String nodeType, // the type of node, eg: datasink
            String nodeName, // the name of the node, eg: logfile
            String fieldName, // the name of the field undergoing conversino, eg: timestamp
            String srcType,
            String destType,
            String input,
            ObjectInstantiator objInst) {
        // Use conversion hint if it exists.
        String hintId = getHintId(nodeType, nodeName, fieldName);
        ConversionFunction cfn = getHint(hintId);
        if (cfn != null) {
            if (!cfn.inputType.equals(srcType) || !cfn.outputType.equals(destType)) {
                logger.error("Type error: conversion hint for " + hintId + " refers to a conversion function named " + cfn.name + " which has type " + cfn.inputType + " -> " + cfn.outputType + " The current conversion needs to convert " + srcType + " -> " + destType + ".");
                return null;
            }
            return getConversionCode(cfn, input, objInst);
        }

        // If no conversion hint is given, but src and dest types are the same, then no conversion is necessary, return the input itself.
        if (srcType.equals(destType)) {
            return input;
        }
        // If a conversion is registered for the source/destination types, then perform it.
        if (this.conversionFunctionsByType.containsKey(srcType) &&
                this.conversionFunctionsByType.get(srcType).containsKey(destType) &&
                this.conversionFunctionsByType.get(srcType).get(destType) != null) {
            return getConversionCode(this.conversionFunctionsByType.get(srcType).get(destType), input, objInst);
        }
        // If no conversion is available, but the destination type is string, then use toString()!
        if (destType.equals("java.lang.String")) {
            logger.debug("No registered conversion for " + srcType + " -> java.lang.String. Using toString()!");
            return getConversionCode(toString, input, objInst);
        }
        // If the conversion could not be performed, return null!
        return null;
    }

    private static String getHintId(String nodeType, String nodeName, String fieldName) {
        return nodeType + ":" + nodeName + ":" + fieldName;
    }

    public void registerConversionHint(String nodeType, String nodeName, ConversionHint hint) {
        String key = getHintId(nodeType, nodeName, hint.getField());
        if (conversionHints.containsKey(key)) {
            logger.warn("Overriding conversion hint for " + key);
        }
        this.conversionHints.put(key, hint);
    }

    private void registerConversionFunction(ConversionFunction cfn) {
        logger.debug("Registering conversion function " + cfn.canonicalClassName + "." + cfn.methodName + " which converts " + cfn.inputType + " -> " + cfn.outputType);
        if (null != cfn.name) {
            this.conversionFunctionsByName.put(cfn.name, cfn);
        }
        if (!this.conversionFunctionsByType.containsKey(cfn.inputType)) {
            this.conversionFunctionsByType.put(cfn.inputType, new java.util.HashMap<String, ConversionFunction>());
        }
        if (this.conversionFunctionsByType.get(cfn.inputType).containsKey(cfn.outputType)) {
            logger.warn("Overriding default conversion function for " + cfn.inputType + " -> " + cfn.outputType);
        }
        this.conversionFunctionsByType.get(cfn.inputType).put(cfn.outputType, cfn);
    }

    private ConversionFunction conversionFunctionFactory(String className, String methodName) {
        try {
        Class convClass = ConversionManager.class.getClassLoader().loadClass(className);
        ConversionFunction cfn = null;
        for (Method m : convClass.getMethods()) {
            if (!m.getName().equals(methodName)) continue;
            ConversionFunction newCfn = conversionFunctionFactory(convClass,m,null);
            if (newCfn == null) continue;
            if (cfn == null) cfn = newCfn;
            if (cfn.type.ordinal() > newCfn.type.ordinal()) {
                logger.warn("Overriding conversion function for "+className+"."+methodName);
                cfn = newCfn;
            }
        }
        return cfn;
        } catch (Exception ex) {
            logger.error("Could not find conversion method "+methodName+" of class "+className, ex);
            return null;
        }
    }

    private static String getClassLoaderName(Class parentClass, Class childClass, String acc) {
        String childClassName = childClass.getCanonicalName();
        if (parentClass == null) {
            return childClassName + ((acc.length()>0)?"$":"")+acc;
        }
        String parentClassName = parentClass.getCanonicalName();
        String innerClassNameOnly = childClassName.substring(parentClassName.length()+1); // +1 to remove separator '.'
        return getClassLoaderName(parentClass.getEnclosingClass(), parentClass, innerClassNameOnly + ((acc.length()>0)?"$":"")+acc);
    }

    private static String getClassLoaderName(Class c) {
        return getClassLoaderName(c.getEnclosingClass(),c,"");
    }

    private ConversionFunction conversionFunctionFactory(Class klass, Method m, String conversionName) {
        ConversionFunction convFn = new ConversionFunction();
        convFn.name = conversionName;
        convFn.canonicalClassName = klass.getCanonicalName();
        convFn.classloaderClassName = getClassLoaderName(klass);
        convFn.methodName = m.getName();
        convFn.inputType = getType(m.getGenericParameterTypes());
        convFn.outputType = getType(m.getGenericReturnType());
        if (Modifier.isStatic(m.getModifiers())) {
            if (m.getGenericParameterTypes().length != 1) {
                logger.error("Static conversion methods should take exactly one argument! " + convFn.canonicalClassName + "." + convFn.methodName + " takes " + m.getGenericParameterTypes().length + ".");
                return null;
            }
            convFn.type = ConversionFunction.Type.STATIC_METHOD;
            return convFn;
        }
        if (m.getGenericParameterTypes().length == 0) {
            convFn.type = ConversionFunction.Type.INSTANCE_METHOD;
            convFn.inputType = klass.getName();
            return convFn;
        } else if (m.getGenericParameterTypes().length == 1) {
            convFn.type = ConversionFunction.Type.CONVERSION_CLASS;
            return convFn;
        }
        logger.error("Non-Static conversion methods should take exactly zero or one argument! " + convFn.canonicalClassName + "." + convFn.methodName + " takes " + m.getGenericParameterTypes().length + ".");
        return null;
    }

    private boolean registerConversionFunction(Class klass, Method m, String conversionName) {
        ConversionFunction convFn = conversionFunctionFactory(klass,m,conversionName);
        if (convFn == null) return false;
        registerConversionFunction(convFn);
        return true;
    }

    // DetectConversionFunctions recursively scans the input class for conversion functions.
    // We can assume that only outer classes or public static inner classes are passed to this function.
    private void detectConversionFunctions(Class klass) {
        // Scan all methods of klass
        for (Method m : klass.getMethods()) {
            int modifiers = m.getModifiers();
            if (!m.isAnnotationPresent(Conversion.class) || !Modifier.isPublic(modifiers)) {
                continue; // We're only interested in public annotated methods.
            }
            registerConversionFunction(klass, m, m.getAnnotation(Conversion.class).name());
        }
        // Scan children as well!
        for (Class c : klass.getClasses()) {
            int modifiers = c.getModifiers();
            // TODO: are top-level classes static? Test for top-level classes!
            if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)) {
                detectConversionFunctions(c);
            }
        }
    }

    public static String getType(Type[] types) {
        java.util.List<String> s = new java.util.LinkedList<String>();
        for (Type t : types) {
            s.add(getType(t));
        }
        return StringUtils.printContainer(s.iterator(), ",");
    }  

    private static Class convertPrimitiveClasses(Class c) {
        if (c.equals(int.class)) return java.lang.Integer.class;
        if (c.equals(long.class)) return java.lang.Long.class;
        if (c.equals(float.class)) return java.lang.Float.class;
        if (c.equals(boolean.class)) return java.lang.Boolean.class;
        if (c.equals(double.class)) return java.lang.Double.class;
        if (c.equals(char.class)) return java.lang.Character.class;
        return c;
    }

    public static String getType(Type t) {

        if (t instanceof ParameterizedType) {
            ParameterizedType type = (ParameterizedType) t;
            Type[] typeArguments = type.getActualTypeArguments();
            java.util.List<String> typeNames = new java.util.LinkedList<String>();
            for (Type typeArgument : typeArguments) {
                typeNames.add(getType(typeArgument));
            }
            return ((Class) type.getRawType()).getName() + "<" + StringUtils.printContainer(typeNames.iterator(), ",") + ">";
        } else {
            try {
                Class c = (Class) t;
                c = convertPrimitiveClasses(c);
                if (c.getComponentType() == null) return c.getCanonicalName();
                return c.getComponentType().getCanonicalName()+"[]";
            } catch (ClassCastException ex1) {
                try {
                    java.lang.reflect.GenericArrayType a = (java.lang.reflect.GenericArrayType) t;
                    return getType(a.getGenericComponentType()) + "[]";
                } catch (ClassCastException ex2) {
                    java.lang.reflect.WildcardType w = (java.lang.reflect.WildcardType) t;
                    return "?";
                }
            }
        }
    }

    // For testing:
    public static void fn1(int a, long b, String c, int[] e, java.util.List<Integer> f, java.util.Map<String, java.util.List<java.util.Set<java.util.Date>>> g, java.util.List<?>[] h) {}
    public static void main(String[] args) {
        for (Method m : ConversionManager.class.getMethods()) {
            //System.out.println(m.getName() + " "+StringUtils.printContainer(Arrays.asList(m.getParameterTypes()).iterator(),","));
            System.out.println(m.getName() + ": " + getType(m.getGenericParameterTypes()));
        }
    }
}
