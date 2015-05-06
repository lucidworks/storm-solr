package com.lucidworks.storm.solr;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultSolrInputDocumentMapper implements SolrInputDocumentMapper {

  public static Logger log = Logger.getLogger(DefaultSolrInputDocumentMapper.class);

  protected boolean fieldGuessingEnabled = false;
  protected String idFieldName = "id";
  protected Map<String, String> dynamicFieldOverrides = null;

  public Map<String, String> getDynamicFieldOverrides() {
    return dynamicFieldOverrides;
  }

  public void setDynamicFieldOverrides(Map<String, String> dynamicFieldOverrides) {
    this.dynamicFieldOverrides = dynamicFieldOverrides;
  }

  public String getIdFieldName() {
    return idFieldName;
  }

  public void setIdFieldName(String idFieldName) {
    this.idFieldName = idFieldName;
  }

  public boolean isFieldGuessingEnabled() {
    return fieldGuessingEnabled;
  }

  public void setFieldGuessingEnabled(boolean fieldGuessingEnabled) {
    this.fieldGuessingEnabled = fieldGuessingEnabled;
  }

  public SolrInputDocument toInputDoc(String docId, Object obj) {
    return (obj instanceof SolrInputDocument) ? (SolrInputDocument) obj : autoMapToSolrInputDoc(docId, obj);
  }

  protected SolrInputDocument map2doc(SolrInputDocument doc, Map map) {
    for (Object key : map.keySet()) {
      doc.setField((String)key, map.get(key));
    }
    return doc;
  }

  protected SolrInputDocument autoMapToSolrInputDoc(String docId, Object obj) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(idFieldName, docId);
    if (obj == null)
      return doc;

    if (obj instanceof Map)
      return map2doc(doc, (Map)obj);

    Class objClass = obj.getClass();
    Set<String> fields = new HashSet<String>();
    Field[] publicFields = obj.getClass().getFields();
    if (publicFields != null) {
      for (Field f : publicFields) {
        // only non-static public
        if (Modifier.isStatic(f.getModifiers()) || !Modifier.isPublic(f.getModifiers()))
          continue;

        Object value = null;
        try {
          value = f.get(obj);
        } catch (IllegalAccessException e) {
        }

        if (value != null) {
          String fieldName = f.getName();
          fields.add(fieldName);

          if (idFieldName.equals(fieldName))
            continue;

          if (fieldGuessingEnabled) {
            if (!f.getType().isArray())
              doc.addField(fieldName, value);
          } else {
            addDynField(doc, fieldName, value, f.getType(),
              (dynamicFieldOverrides != null) ? dynamicFieldOverrides.get(fieldName) : null);
          }
        }
      }
    }

    PropertyDescriptor[] props = null;
    try {
      BeanInfo info = Introspector.getBeanInfo(objClass);
      props = info.getPropertyDescriptors();
    } catch (IntrospectionException e) {
      log.warn("Can't get BeanInfo for class: " + objClass);
    }

    if (props != null) {
      for (PropertyDescriptor pd : props) {
        String propName = pd.getName();
        if ("class".equals(propName) || fields.contains(propName))
          continue;

        Method readMethod = pd.getReadMethod();
        if (readMethod != null) {
          Object value = null;
          try {
            value = readMethod.invoke(obj);
          } catch (Exception e) {
            log.debug("Failed to invoke read method for property '" + pd.getName() +
              "' on object of type '" + objClass.getName() + "' due to: " + e);
          }

          if (value != null) {
            fields.add(propName);
            if (idFieldName.equals(propName))
              continue;

            if (fieldGuessingEnabled) {
              if (!pd.getPropertyType().isArray())
                doc.addField(propName, value);
            } else {
              addDynField(doc, propName, value, pd.getPropertyType(),
                (dynamicFieldOverrides != null) ? dynamicFieldOverrides.get(propName) : null);
            }

          }
        }
      }
    }

    return doc;
  }

  protected void addDynField(SolrInputDocument doc,
                             String fieldName,
                             Object value,
                             Class type,
                             String dynamicFieldSuffix) {
    if (type.isArray())
      return; // TODO: Array types not supported yet ...

    if (dynamicFieldSuffix == null) {
      dynamicFieldSuffix = getDefaultDynamicFieldMapping(type);
      // treat strings with multiple terms as text only if using the default!
      if ("_s".equals(dynamicFieldSuffix)) {
        String str = (String) value;
        if (str.indexOf(" ") != -1)
          dynamicFieldSuffix = "_t";
      }
    }

    if (dynamicFieldSuffix != null) // don't auto-map if we don't have a type
      doc.addField(fieldName + dynamicFieldSuffix, value);
  }

  protected String getDefaultDynamicFieldMapping(Class clazz) {
    if (String.class.equals(clazz))
      return "_s";
    else if (Long.class.equals(clazz) || long.class.equals(clazz))
      return "_l";
    else if (Integer.class.equals(clazz) || int.class.equals(clazz))
      return "_i";
    else if (Double.class.equals(clazz) || double.class.equals(clazz))
      return "_d";
    else if (Float.class.equals(clazz) || float.class.equals(clazz))
      return "_f";
    else if (Boolean.class.equals(clazz) || boolean.class.equals(clazz))
      return "_b";
    else if (Date.class.equals(clazz))
      return "_tdt";
    return null; // default is don't auto-map
  }
}
