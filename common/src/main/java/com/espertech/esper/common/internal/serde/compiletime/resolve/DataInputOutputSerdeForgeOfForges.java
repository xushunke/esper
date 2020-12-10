/*
 ***************************************************************************************
 *  Copyright (C) 2006 EsperTech, Inc. All rights reserved.                            *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 ***************************************************************************************
 */
package com.espertech.esper.common.internal.serde.compiletime.resolve;

import com.espertech.esper.common.internal.bytecodemodel.base.CodegenClassScope;
import com.espertech.esper.common.internal.bytecodemodel.base.CodegenMethod;
import com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpression;

import static com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpressionBuilder.newInstance;

public class DataInputOutputSerdeForgeOfForges implements DataInputOutputSerdeForge {
    private final String forgeClassName;
    private final DataInputOutputSerdeForge[] forges;

    public DataInputOutputSerdeForgeOfForges(String forgeClassName, DataInputOutputSerdeForge[] forges) {
        this.forgeClassName = forgeClassName;
        this.forges = forges;
    }

    public String forgeClassName() {
        return forgeClassName;
    }

    public DataInputOutputSerdeForge[] getForges() {
        return forges;
    }

    public CodegenExpression codegen(CodegenMethod method, CodegenClassScope classScope, CodegenExpression optionalEventTypeResolver) {
        return newInstance(forgeClassName, optionalEventTypeResolver);
    }
}
