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
package com.espertech.esper.common.internal.epl.agg.core;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.type.EPTypePremade;
import com.espertech.esper.common.internal.bytecodemodel.base.CodegenClassScope;
import com.espertech.esper.common.internal.bytecodemodel.base.CodegenMethod;
import com.espertech.esper.common.internal.bytecodemodel.core.CodegenNamedMethods;
import com.espertech.esper.common.internal.bytecodemodel.core.CodegenNamedParam;
import com.espertech.esper.common.internal.bytecodemodel.core.CodegenTypedParam;
import com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpression;
import com.espertech.esper.common.internal.compile.multikey.MultiKeyClassRef;
import com.espertech.esper.common.internal.epl.expression.codegen.ExprForgeCodegenSymbol;
import com.espertech.esper.common.internal.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.common.internal.epl.expression.core.ExprNode;
import com.espertech.esper.common.internal.fabric.FabricTypeCollector;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static com.espertech.esper.common.internal.bytecodemodel.model.expression.CodegenExpressionBuilder.*;
import static com.espertech.esper.common.internal.epl.expression.codegen.ExprForgeCodegenNames.*;

public class AggregationServiceCodegenUtil {
    public static CodegenMethod computeMultiKeyCodegen(int idNumber, ExprNode[] partitionForges, MultiKeyClassRef optionalMultiKey, CodegenClassScope classScope, CodegenNamedMethods namedMethods) {
        ExprForgeCodegenSymbol exprSymbol = new ExprForgeCodegenSymbol(true, null);
        Consumer<CodegenMethod> code = method -> {

            if (optionalMultiKey == null || optionalMultiKey.getClassNameMK() == null) {
                CodegenExpression expression = partitionForges[0].getForge().evaluateCodegen(EPTypePremade.OBJECT.getEPType(), method, exprSymbol, classScope);
                exprSymbol.derivedSymbolsCodegen(method, method.getBlock(), classScope);
                method.getBlock().methodReturn(expression);
                return;
            }

            CodegenExpression[] expressions = new CodegenExpression[partitionForges.length];
            for (int i = 0; i < partitionForges.length; i++) {
                expressions[i] = partitionForges[i].getForge().evaluateCodegen(EPTypePremade.OBJECT.getEPType(), method, exprSymbol, classScope);
            }
            exprSymbol.derivedSymbolsCodegen(method, method.getBlock(), classScope);
            method.getBlock().methodReturn(newInstance(optionalMultiKey.getClassNameMK(), expressions));
        };

        return namedMethods.addMethodWithSymbols(EPTypePremade.OBJECT.getEPType(), "computeKeyArrayCodegen_" + idNumber, CodegenNamedParam.from(EventBean.EPTYPEARRAY, NAME_EPS, EPTypePremade.BOOLEANPRIMITIVE.getEPType(), NAME_ISNEWDATA, ExprEvaluatorContext.EPTYPE, NAME_EXPREVALCONTEXT), AggregationServiceCodegenUtil.class, classScope, code, exprSymbol);
    }

    public static void generateIncidentals(boolean hasRefcount, boolean hasLastUpdTime, AggregationRowCtorDesc rowCtorDesc) {
        CodegenNamedMethods namedMethods = rowCtorDesc.getNamedMethods();
        CodegenClassScope classScope = rowCtorDesc.getClassScope();
        List<CodegenTypedParam> rowMembers = rowCtorDesc.getRowMembers();

        if (hasRefcount) {
            rowMembers.add(new CodegenTypedParam(EPTypePremade.INTEGERPRIMITIVE.getEPType(), "refcount").setFinal(false));
        }
        namedMethods.addMethod(EPTypePremade.VOID.getEPType(), "increaseRefcount", Collections.emptyList(), AggregationServiceCodegenUtil.class, classScope,
            hasRefcount ? method -> method.getBlock().increment(ref("refcount")) : method -> {
            });
        namedMethods.addMethod(EPTypePremade.VOID.getEPType(), "decreaseRefcount", Collections.emptyList(), AggregationServiceCodegenUtil.class, classScope,
            hasRefcount ? method -> method.getBlock().decrement(ref("refcount")) : method -> {
            });
        namedMethods.addMethod(EPTypePremade.LONGPRIMITIVE.getEPType(), "getRefcount", Collections.emptyList(), AggregationServiceCodegenUtil.class, classScope,
            hasRefcount ? method -> method.getBlock().methodReturn(ref("refcount")) : method -> method.getBlock().methodReturn(constant(1)));

        if (hasLastUpdTime) {
            rowMembers.add(new CodegenTypedParam(EPTypePremade.LONGPRIMITIVE.getEPType(), "lastUpd").setFinal(false));
        }
        namedMethods.addMethod(EPTypePremade.VOID.getEPType(), "setLastUpdateTime", CodegenNamedParam.from(EPTypePremade.LONGPRIMITIVE.getEPType(), "time"), AggregationServiceCodegenUtil.class, classScope,
                hasLastUpdTime ? method -> method.getBlock().assignRef("lastUpd", ref("time")) : method -> method.getBlock().methodThrowUnsupported());
        namedMethods.addMethod(EPTypePremade.LONGPRIMITIVE.getEPType(), "getLastUpdateTime", Collections.emptyList(), AggregationServiceCodegenUtil.class, classScope,
                hasLastUpdTime ? method -> method.getBlock().methodReturn(ref("lastUpd")) : method -> method.getBlock().methodThrowUnsupported());
    }

    public static void appendIncidentals(boolean hasRefcount, boolean hasLastUpdTime, FabricTypeCollector fabricTypeCollector) {
        if (hasRefcount) {
            fabricTypeCollector.builtin(int.class);
        }
        if (hasLastUpdTime) {
            fabricTypeCollector.builtin(long.class);
        }
    }
}
