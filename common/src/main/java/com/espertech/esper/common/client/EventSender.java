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
package com.espertech.esper.common.client;

import java.util.HashSet;
import java.util.Set;

/**
 * Returns a facility to process event objects that are of a known type.
 * <p>
 * Obtained via the method EPRuntime#getEventSender(String) the sender is specific to a given
 * event type and may not process event objects of any other event type; See the method documentation for more details.
 */
public interface EventSender {
    ThreadLocal<Set<String>> THREAD_DISABLED_STATEMENT_NAMES = new ThreadLocal<>();

    static Set<String> disabledNames() {
        return new HashSet<>(THREAD_DISABLED_STATEMENT_NAMES.get());
    }

    static void setDisabledNames(Set<String> disabledNames) {
        THREAD_DISABLED_STATEMENT_NAMES.set(disabledNames);
    }

    /**
     * Processes the event object.
     * <p>
     * Use the route method for sending events into the runtime from within UpdateListener code.
     * to avoid the possibility of a stack overflow due to nested calls to sendEvent.
     *
     * @param theEvent to process
     * @throws EPException if a runtime error occured.
     */
    void sendEvent(Object theEvent) throws EPException;

    /**
     * Route the event object back to the event stream processing runtime for internal dispatching,
     * to avoid the possibility of a stack overflow due to nested calls to sendEvent.
     * The route event is processed just like it was sent to the runtime, that is any
     * active expressions seeking that event receive it. The routed event has priority over other
     * events sent to the runtime. In a single-threaded application the routed event is
     * processed before the next event is sent to the runtime through the
     * EPRuntime.sendEvent method.
     *
     * @param theEvent to process
     * @throws EPException is thrown when the processing of the event lead to an error
     */
    void routeEvent(Object theEvent) throws EPException;
}
