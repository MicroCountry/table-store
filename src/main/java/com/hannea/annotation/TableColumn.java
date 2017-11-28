package com.hannea.annotation;

import com.hannea.constant.ColumnTypeObject;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Inherited
public @interface TableColumn {
	String name() default "";

	ColumnTypeObject type() default ColumnTypeObject.STRING;
}
