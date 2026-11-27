/*
 * *****************************************************************************
 * Copyright (C) 2014-2026 Dennis Sheirer
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 * ****************************************************************************
 */

package io.github.dsheirer.health;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Running totals of how well one standard (not traffic) channel is heard, since it started: as a trunked site's
 * control channel (control messages decoded and lost, seconds locked, the site's identity, the calls it announced) or
 * as a conventional channel (seconds running, openings, seconds open).  A channel counts as a site once it has decoded
 * a control message.
 *
 * Written by the channel's HealthMonitor and its traffic channel manager, read by the HealthRegistry.  Thread safe.
 */
public class ChannelHealth
{
    /** A control channel counts as locked until this long after its last good message. */
    public static final long LOCK_HOLD_MS = 2000;
    /** A decoder that has said nothing (no message, no sync loss) for this long isn't locked. */
    public static final long SILENT_MS = 10_000;

    private final ChannelInfo mInfo;
    private final CallTally mCalls = new CallTally();
    private boolean mCallsCounted;

    private String mProtocol;
    private long mFrequency;
    private final Map<String,Integer> mIdentity = new TreeMap<>();
    private long mGood;
    private long mBad;
    private long mSyncLossSym;
    private boolean mSyncLossMeasured;
    private long mLockedMs;
    private long mLastGood;
    private long mLastHeard;
    private long mLastHeardAt;

    private boolean mRunning;
    private long mRunningMs;
    private long mRunningSince;
    private long mOpens;
    private long mCloses;
    private long mOpenMs;
    private long mOpenedAt;

    public ChannelHealth(ChannelInfo info)
    {
        mInfo = info;
    }

    public ChannelInfo getInfo()
    {
        return mInfo;
    }

    /**
     * Calls the channel's control channel announced.
     */
    public CallTally getCalls()
    {
        return mCalls;
    }

    /**
     * Something counts the calls this channel's control channel announces (its traffic channel manager).  Until then
     * a site's calls aren't known, and a report leaves the site out rather than say it had none.
     */
    public synchronized void setCallsCounted()
    {
        mCallsCounted = true;
    }

    /**
     * The channel started (or restarted) processing.
     */
    public synchronized void started(long now)
    {
        if(!mRunning)
        {
            mRunning = true;
            mRunningSince = now;
        }
    }

    /**
     * The channel stopped processing.  An open squelch is cut off: its time counts as open, but it isn't counted as
     * a transmission completed.
     */
    public synchronized void stopped(long now)
    {
        if(mRunning)
        {
            mRunningMs += Math.max(0, now - mRunningSince);
            mRunning = false;
        }

        if(mOpenedAt > 0)
        {
            mOpenMs += Math.max(0, now - mOpenedAt);
            mOpenedAt = 0;
        }
    }

    /**
     * The frequency the channel is tuned to now (it can rotate through a site's control channels).
     */
    public synchronized void setFrequency(long frequency)
    {
        mFrequency = frequency;
    }

    /**
     * A control message was framed, as it arrives (a message's timestamp in a test).
     */
    public synchronized void control(String protocol, boolean valid, long timestamp)
    {
        control(protocol, valid, timestamp, timestamp);
    }

    /**
     * A control message was framed.
     * @param protocol of the site, as a node health report names it (p25, dmr, ...)
     * @param valid true when it decoded, false when it failed its CRC or FEC
     * @param timestamp of the message: when its samples were received, which falls behind the time it's decoded by
     *                  however long the samples waited to be processed
     * @param arrivedAt when the decoder gave it, Unix milliseconds
     */
    public synchronized void control(String protocol, boolean valid, long timestamp, long arrivedAt)
    {
        mProtocol = protocol;
        heard(timestamp, arrivedAt);

        if(valid)
        {
            mGood++;

            if(mLastGood > 0 && timestamp > mLastGood)
            {
                mLockedMs += Math.min(timestamp - mLastGood, LOCK_HOLD_MS);
            }

            mLastGood = Math.max(mLastGood, timestamp);
        }
        else
        {
            mBad++;
        }
    }

    /**
     * The channel's decoder says when it goes without frame sync (not every decoder does).
     */
    public synchronized void setSyncLossMeasured(boolean measured)
    {
        mSyncLossMeasured = measured;
    }

    /**
     * The decoder said something, at a time on its messages' clock: a message, or that it went without sync.
     */
    private void heard(long timestamp, long arrivedAt)
    {
        mLastHeard = Math.max(mLastHeard, timestamp);
        mLastHeardAt = Math.max(mLastHeardAt, arrivedAt);
    }

    /**
     * The control channel's decoder went without frame sync.
     * @param symbols it went through without sync
     */
    public synchronized void syncLoss(long symbols)
    {
        addSyncLoss(symbols);
    }

    /**
     * The control channel's decoder went without frame sync.
     * @param symbols it went through without sync
     * @param timestamp of the sync loss message
     * @param arrivedAt when the decoder gave it, Unix milliseconds
     */
    public synchronized void syncLoss(long symbols, long timestamp, long arrivedAt)
    {
        heard(timestamp, arrivedAt);
        addSyncLoss(symbols);
    }

    private void addSyncLoss(long symbols)
    {
        if(symbols > 0)
        {
            mSyncLossSym += symbols;
        }
    }

    /**
     * Part of the site's identity, as its control channel broadcasts it about itself (never from a neighbour's).
     * @param name of the field (wacn, sysId, rfss, site, nac, colorCode, ...)
     * @param value of the field
     */
    public synchronized void identity(String name, int value)
    {
        mIdentity.put(name, value);
    }

    /**
     * Squelch opened (a conventional channel).
     */
    public synchronized void opened(long now)
    {
        if(mOpenedAt == 0)
        {
            mOpens++;
            mOpenedAt = now;
        }
    }

    /**
     * Squelch closed (a conventional channel).
     */
    public synchronized void closed(long now)
    {
        if(mOpenedAt > 0)
        {
            mOpenMs += Math.max(0, now - mOpenedAt);
            mCloses++;
            mOpenedAt = 0;
        }
    }

    /**
     * The totals at this moment.
     */
    public synchronized Totals totals(long now)
    {
        long locked = mLockedMs;

        if(mLastGood > 0 && now > mLastGood)
        {
            locked += Math.min(now - mLastGood, LOCK_HOLD_MS);
        }

        //Locked now: the decoder's latest word was within 2 s of a good message, on its messages' own clock (they
        //run behind this one by however long samples wait to be processed), and it's still saying something.
        boolean lockedNow = mRunning && mLastGood > 0 && mLastHeard - mLastGood <= LOCK_HOLD_MS &&
            now - mLastHeardAt <= SILENT_MS;
        long running = mRunningMs + (mRunning ? Math.max(0, now - mRunningSince) : 0);
        long open = mOpenMs + (mOpenedAt > 0 ? Math.max(0, now - mOpenedAt) : 0);

        return new Totals(mInfo, mProtocol, mFrequency, Collections.unmodifiableMap(new TreeMap<>(mIdentity)), mGood,
            mBad, mSyncLossMeasured ? mSyncLossSym : null, locked, lockedNow, mCallsCounted ? mCalls.totals() : null,
            running, mOpens, mCloses, open);
    }

    /**
     * What the channel is, from its configuration.
     * @param channelId of the channel configuration, for this run
     * @param configurationId the channel configuration's durable ID (a UUID), or null
     * @param aliasListId durable ID of the channel's alias list (which aliases, and so which streams, its calls go to)
     * @param name of the channel
     * @param system name the user gave
     * @param site name the user gave
     * @param aliasListName of the channel
     * @param mode a conventional channel's mode as a node health report names it (fm, am, p25, dmr, nxdn), or null
     *             when the decoder can't be a conventional channel
     * @param symbolRate of its control channel, baud, or null when not known
     */
    public record ChannelInfo(int channelId, String configurationId, long aliasListId, String name, String system,
                              String site, String aliasListName, String mode, Integer symbolRate)
    {
    }

    /**
     * One channel's totals at one moment, since it started.
     * @param protocol of the site, or null when no control message has been decoded (a conventional channel)
     * @param frequency tuned
     * @param identity of the site, as broadcast
     * @param good control messages decoded
     * @param bad control messages framed but not decoded
     * @param syncLossSym symbols the control channel's decoder went through without frame sync, or null when its
     *                    decoder doesn't say
     * @param lockedMs milliseconds with a good control message within the last 2 s
     * @param locked true when the decoder's latest message (or sync loss) came within 2 s of a good control message
     *               and it gave one within the last 10 s: false while the channel hunts for its control channel
     * @param calls announced, and their outcomes, or null when nothing counts them
     * @param runningMs milliseconds the channel was processing
     * @param opens squelch openings
     * @param closes squelch closings (transmissions completed)
     * @param openMs milliseconds open
     */
    public record Totals(ChannelInfo info, String protocol, long frequency, Map<String,Integer> identity, long good,
                         long bad, Long syncLossSym, long lockedMs, boolean locked, CallTally.Totals calls,
                         long runningMs,
                         long opens, long closes, long openMs)
    {
        /**
         * True when the channel has decoded control messages: a trunked site.
         */
        public boolean isSite()
        {
            return protocol != null;
        }
    }
}
