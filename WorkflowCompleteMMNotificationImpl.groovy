/*
Copyright (c) 2016, NextGate Solutions All Rights Reserved.

This program, and all the NextGate Solutions authored routines referenced herein,
are the proprietary properties and trade secrets of NextGate Solutions.

Except as provided for by license agreement, this program shall not be duplicated,
used, reversed engineered, decompiled, disclosed, shared, transferred, placed in a
publicly available location (such as but not limited to ftp sites, bit torrents,
shared drives, peer to peer networks, and such) without  written consent signed by
an officer of NextGate Solutions.

DISCLAIMER:

This software is provided as is without warranty of any kind. The entire risk as to
the results and performance of this software is assumed by the licensee and/or its
affiliates and/or assignees. NextGate Solutions disclaims all warranties, either
expressed or implied, including but not limited to the implied warranties of
merchantability, fitness for a particular purpose, title and non-infringement, with
respect to this software.

- version control -
$Id: WorkflowCompleteMMNotificationImpl.groovy 35955 2018-10-25 00:38:18Z kevin.schmidt $
 */

package com.nextgate.ms.component.adapter.sender.workflow.impl;

import java.util.HashSet
import java.util.Set

import org.apache.camel.Exchange
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.nextgate.core.exceptions.InvalidParameterException
import com.nextgate.core.exceptions.ResourceUnavailableException
import com.nextgate.interfaces.ms.HeaderTags;
import com.nextgate.interfaces.ms.datatypes.ComparisonType;
import com.nextgate.interfaces.ms.datatypes.NGMSMessage
import com.nextgate.interfaces.ms.datatypes.MMNotificationResponse;
import com.nextgate.interfaces.ngs.enums.NotificationEventTypeEnum;
import com.nextgate.interfaces.ngs.enums.RegistryType;
import com.nextgate.ms.component.adapter.sender.workflow.WorkflowConstants
import com.nextgate.ms.component.adapter.sender.workflow.WorkflowSenderFactory
import com.nextgate.ms.component.adapter.sender.workflow.interfaces.WorkflowCompleteContext
import com.nextgate.ms.component.adapter.sender.workflow.interfaces.WorkflowCompleteGenerator
import com.nextgate.ms.component.adapter.sender.workflow.utils.WorkflowHelper
import com.nextgate.ms.configmanager.cache.ConfigurationCacheEntry
import com.nextgate.ms.core.LogConstants
import com.nextgate.ms.core.NGMSConstants
import com.nextgate.ms.core.utils.CamelUtil;




/**
 * Customizable class to handle MM Notification events to Workflow.
 * 
 * @author Peter Berkman
 * @version $Revision: 35955 $
 * @since 10.0.0
 */
public class WorkflowCompleteMMNotificationImpl implements WorkflowCompleteGenerator {

    private static final transient Logger LOG = LoggerFactory.getLogger(WorkflowCompleteMMNotificationImpl.class);
    
    public WorkflowCompleteMMNotificationImpl() {
        
        // nothing yet.
    }
    
    void process(WorkflowCompleteContext context) throws InvalidParameterException, ResourceUnavailableException {
        
        String currentRoute = CamelUtil.getRouteName(context.getExch());
        LOG.trace(LogConstants.EXECBEGIN, context.getExch().getContext().getName(), currentRoute);
        LOG.trace(LogConstants.ROUTINGTRIGGERIN, context.getExch().getIn().getHeader(HeaderTags.ROUTING_TRIGGER));

        if (context.getMsMsg().getMsAdapterReplyObject() == null) {

            throw new InvalidParameterException("The msAdapterReplyObject from NGMSMessage is null.  FATAL.");
        }

        if (!(context.getMsMsg().getMsAdapterReplyObject() instanceof MMNotificationResponse)) {

            throw new InvalidParameterException("The msAdapterReplyObject from NGMSMessage is an unsupported type.  FATAL.");
        }

        MMNotificationResponse request = (MMNotificationResponse) context.getMsMsg().getMsAdapterReplyObject();

        //
        // make sure we need to process the notification
        //

        if ((request.countPotentialDupsNew() == 0) &&
            (request.countPotentialDupsDeleted() == 0) &&
            (request.countPotentialDupsUpdated() == 0) &&
            (request.countPotentialDupsResolved() == 0) &&
            (request.countPotentialDupsPermResolved() == 0) &&
            (request.countPotentialDupsUnresolved() == 0) &&
            (request.getAssumedMatch() == null) &&
            (request.getPotentialOverlay() == null) &&
            (request.getEvent() != NotificationEventTypeEnum.MRG_EO)) {

            LOG.debug("Nothing to do for event [{}]: {}", request.getEvent(), context.getMsMsg());
            LOG.trace(LogConstants.EXECEND, context.getExch().getContext().getName(), currentRoute);

            return;
        }

        //
        // make sure we are looking at the right registry
        //

        if ((request.getRegistry() == RegistryType.PERSON) && !(NGMSConstants.PERSON.equals(context.getWsig().getAppName()))) {

            throw new InvalidParameterException("Received a notification for Person, but we are NOT in the Person route.");
        }

        if ((request.getRegistry() == RegistryType.PROVIDER) && !(NGMSConstants.PROVIDER.equals(context.getWsig().getAppName()))) {

            throw new InvalidParameterException("Received a notification for Provider, but we are NOT in the Provider route.");
        }

        request.toTraceLog(LOG, LogConstants.FOURSPACES);

        //
        // process workflow tasks
        //

        if (request.countPotentialDupsNew() > 0) {

            this.processPotentialDupsNew(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.countPotentialDupsDeleted() > 0) {

            this.processPotentialDupsDeleted(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.countPotentialDupsUpdated() > 0) {

            this.processPotentialDupsUpdated(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.countPotentialDupsResolved() > 0) {

            this.processPotentialDupsResolved(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.countPotentialDupsPermResolved() > 0) {

            this.processPotentialDupsPermResolved(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.countPotentialDupsUnresolved() > 0) {

            this.processPotentialDupsUnresolved(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.getAssumedMatch() != null) {

            this.processAssumedMatches(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.getPotentialOverlay() != null) {

            this.processPotentialOverlay(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        if (request.getEvent() == NotificationEventTypeEnum.MRG_EO) {

            this.processMerge(context.getConf(), context.getExch(), context.getMsMsg(), request);
        }

        //
        // and return
        //

        LOG.trace(LogConstants.EXECEND, context.getExch().getContext().getName(), currentRoute);
    }

    private void processPotentialDupsNew(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialDupsNew: transid [{}]", msMsg.getMessageControlId());

        for (ComparisonType compt : req.getPotentialDupsNew()) {

            WorkflowHelper.getDefaultProcessors().startPotentialMatch(conf, exch, msMsg, req, compt);
            WorkflowHelper.getDefaultProcessors().startPotentialDuplicate(conf, exch, msMsg, req, compt);
        }
    }
    
    private void processPotentialDupsDeleted(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialDupsDeleted: transid [{}]", msMsg.getMessageControlId());

        Set<String> instanceIds = new HashSet<>();

        for (ComparisonType compt : req.getPotentialDupsDeleted()) {
            
            //NOTE: As a short-term fix, pass the DELETEPI action, but that means this logic is in WU which means it has to change when BPs change or want different behavior.
            WorkflowHelper.getDefaultProcessors().completePotentialMatch(conf, exch, msMsg, req, compt, WorkflowConstants.T_DELETEPI, instanceIds);
            WorkflowHelper.getDefaultProcessors().completePotentialDuplicate(conf, exch, msMsg, req, compt, WorkflowConstants.T_DELETEPI, instanceIds);

        }
    }

    private void processPotentialDupsUpdated(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialDupsUpdated: transid [{}]", msMsg.getMessageControlId());

        Set<String> instanceIds = new HashSet<>();

        for (ComparisonType compt : req.getPotentialDupsUpdated()) {

            WorkflowHelper.getDefaultProcessors().completePotentialMatch(conf, exch, msMsg, req, compt, WorkflowConstants.T_UPDATETASK, instanceIds);
            WorkflowHelper.getDefaultProcessors().completePotentialDuplicate(conf, exch, msMsg, req, compt, WorkflowConstants.T_UPDATETASK, instanceIds);

            // process updated as new too since the update could be due to marked unique going away so need to be recreated

            WorkflowHelper.getDefaultProcessors().startPotentialMatch(conf, exch, msMsg, req, compt);
            WorkflowHelper.getDefaultProcessors().startPotentialDuplicate(conf, exch, msMsg, req, compt);
        }
    }

    private void processPotentialDupsResolved(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialDupsResolved: transid [{}]", msMsg.getMessageControlId());

        Set<String> instanceIds = new HashSet<>();

        for (ComparisonType compt : req.getPotentialDupsResolved()) {

            WorkflowHelper.getDefaultProcessors().completePotentialMatch(conf, exch, msMsg, req, compt, WorkflowConstants.T_EXTERNALUNIQUE, instanceIds);
            WorkflowHelper.getDefaultProcessors().completePotentialDuplicate(conf, exch, msMsg, req, compt, WorkflowConstants.T_EXTERNALUNIQUE, instanceIds);
        }
    }

    private void processPotentialDupsPermResolved(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialDupsPermResolved: transid [{}]", msMsg.getMessageControlId());

        Set<String> instanceIds = new HashSet<>();
        String appName = req.getApplication();

        for (ComparisonType compt : req.getPotentialDupsPermResolved()) {
            // When a mark unique is initiated from Task Manager, the application name is an empty string. For APIs or DQM, the application is NGMMPERSON.REST API and MatchMetrixDQM respectively.
            if (appName != null && !appName.isEmpty()) {
                WorkflowHelper.getDefaultProcessors().completePotentialMatch(conf, exch, msMsg, req, compt, WorkflowConstants.T_DELETEPI, instanceIds);
                WorkflowHelper.getDefaultProcessors().completePotentialDuplicate(conf, exch, msMsg, req, compt, WorkflowConstants.T_DELETEPI, instanceIds);
            }
        }
    }

    private void processPotentialDupsUnresolved(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialDupsUnresolved: transid [{}]", msMsg.getMessageControlId());

        Set<String> instanceIds = new HashSet<>();

        for (ComparisonType compt : req.getPotentialDupsUnresolved()) {
            // delete all open review mark unique tasks
            WorkflowHelper.getDefaultProcessors().completePotentialMatch(conf, exch, msMsg, req, compt, WorkflowConstants.T_EXTERNALNOTUNIQUE, instanceIds);
            WorkflowHelper.getDefaultProcessors().completePotentialDuplicate(conf, exch, msMsg, req, compt, WorkflowConstants.T_EXTERNALNOTUNIQUE, instanceIds);
            WorkflowHelper.getDefaultProcessors().completePotentialMatch(conf, exch, msMsg, req, compt, WorkflowConstants.T_DELETEPI, instanceIds);
            WorkflowHelper.getDefaultProcessors().completePotentialDuplicate(conf, exch, msMsg, req, compt, WorkflowConstants.T_DELETEPI, instanceIds);

            // reopen as new now that the perm mark unique no longer exists
            WorkflowHelper.getDefaultProcessors().startPotentialMatch(conf, exch, msMsg, req, compt);
            WorkflowHelper.getDefaultProcessors().startPotentialDuplicate(conf, exch, msMsg, req, compt);
        }
    }

    private void processAssumedMatches(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processAssumedMatches: transid [{}]", msMsg.getMessageControlId());
        if(req.getEvent() == NotificationEventTypeEnum.MATCH) {
            WorkflowHelper.getDefaultProcessors().startAssumedMatch(conf, exch, msMsg, req, req.getAssumedMatch(), true);
        }
        
        else if(req.getEvent() == NotificationEventTypeEnum.MRG_EO_ON_UPD) {
            WorkflowHelper.getDefaultProcessors().startAssumedMatch(conf, exch, msMsg, req, req.getAssumedMatch(), false);
        }
    }

    private void processPotentialOverlay(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processPotentialOverlay: transid [{}]", msMsg.getMessageControlId());
        WorkflowHelper.getDefaultProcessors().startPotentialOverlay(conf, exch, msMsg, req);
    }

    private void processMerge(final ConfigurationCacheEntry conf, final Exchange exch, final NGMSMessage msMsg, final MMNotificationResponse req) throws Exception {

        LOG.debug("processMerge: transid [{}]", msMsg.getMessageControlId());

        WorkflowHelper.getDefaultProcessors().startMergeComplete(conf, exch, msMsg, req);
    }
}
