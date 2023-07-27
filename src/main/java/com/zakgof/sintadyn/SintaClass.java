package com.zakgof.sintadyn;

import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class SintaClass<V> {

    private static final Objenesis objenesis = new ObjenesisStd();

    private final ObjectInstantiator<V> objenesisInstantiator;
    private final List<Field> propertyFields;
    private final Field keyField;

    SintaClass(Class<V> valueClass, boolean requiresKey) {
        this.objenesisInstantiator = objenesis.getInstantiatorOf(valueClass);
        this.propertyFields = getAllFields(valueClass);
        this.keyField = requiresKey ? getKeyField(propertyFields) : null;
    }

    private AttributeValue getPropertyValue(Field field, V value) {
        return convertAttribute(getFieldValue(field, value), field.getType());
    }

    private <F> F getFieldValue(Field field, V value) {
        try {
            //noinspection unchecked
            return (F) field.get(value);
        } catch (IllegalAccessException e) {
            throw new SintadynException(e);
        }
    }

    private <F> void setFieldValue(Field field, F fieldValue, V value) {
        try {
            field.set(value, fieldValue);
        } catch (IllegalAccessException e) {
            throw new SintadynException(e);
        }
    }

    private AttributeValue convertAttribute(Object attribute, Class<?> type) {
        if (type == int.class || type == Integer.class
                || type == Byte.class || type == byte.class
                || type == Short.class || type == short.class
                || type == Long.class || type == long.class
                || type == Character.class || type == char.class) {
            return AttributeValue.builder().n(attribute.toString()).build(); // TODO: nulls
        } else if (type == String.class) {
            return AttributeValue.builder().s((String) attribute).build();
        } else if (type == Boolean.class) {
            return AttributeValue.builder().bool((Boolean) attribute).build();
        } else if (type == byte[].class) {
            return AttributeValue.builder().b(SdkBytes.fromByteArray((byte[]) attribute)).build();
        } else throw new SintadynException("Non-mappable attribute type:" + type);
        // TODO : array types
        // TODO : built-in convertors
        // TODO : custom convertors
    }

    private <F> F deconvertAttribute(AttributeValue attributeValue, Class<F> type) {
        if (type == String.class) {
            return type.cast(attributeValue.s());
        }
        if (type == int.class) {
            return (F)(Integer)Integer.parseInt(attributeValue.n());
        }
        throw new SintadynException("Non-mappable attribute type:" + type);
        // TODO: all other types
    }

    static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Field[] declaredFields = type.getDeclaredFields();
        Arrays.sort(declaredFields, Comparator.comparing(Field::getName));
        for (Field field : declaredFields) {
            if ((field.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) {
                field.setAccessible(true);
                fields.add(field);
            }
        }
        if (type.getSuperclass() != null)
            fields.addAll(getAllFields(type.getSuperclass()));
        return fields;
    }

    private Field getKeyField(List<Field> propertyFields) {
        Set<Field> keyFields = propertyFields.stream()
                .filter(field -> field.getAnnotation(Key.class) != null)
                .collect(Collectors.toSet());
        if (keyFields.isEmpty()) {
            throw new SintadynException("No key field found");
        } else if (keyFields.size() > 1) {
            throw new SintadynException("Multiple key fields found");
        }
        Field keyField = keyFields.iterator().next();
        propertyFields.remove(keyField); // TODO: performance optimization
        return keyField;
    }

    Map<String, AttributeValue> deflateProperties(V value) {
        return propertyFields.stream()
                .collect(Collectors.toMap(Field::getName, field -> getPropertyValue(field, value)));
    }

    public <K> K keyOf(V value) {
        return getFieldValue(keyField, value);
    }

    <K> String deflateKey(K key) {
        return key.toString(); // TODO !!!!
    }

    V inflate(Sintadyn sintadyn, Map<String, AttributeValue> item) {
        V value = objenesisInstantiator.newInstance();
        propertyFields.forEach(field -> {
            AttributeValue attributeValue = item.get(field.getName());
            Object fieldValue = deconvertAttribute(attributeValue, field.getType());
            setFieldValue(field, fieldValue, value);
        });
        if (keyField != null) {
            AttributeValue keyAttributeValue = item.get(sintadyn.pk());
            Object keyValue = deconvertAttribute(keyAttributeValue, keyField.getType());
            String keyFieldValue = keyValue.toString().split("#")[1]; // TODO!!!
            setFieldValue(keyField, keyFieldValue, value); // TODO
        }
        return value;
    }

}
