package vn.inews.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.AbstractFetchSchedule;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.FetchSchedule;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by Nha.HV <a href="nhahv@tintuc.vn">nhahv@tintuc.vn</a> on 31/08/2015.
 */

public class FixedFetchSchedule extends AbstractFetchSchedule {

	// Logger
	public static final Logger LOG = LoggerFactory
			.getLogger(FixedFetchSchedule.class);

	protected String[] TIME_STEPS_CONFIG;

	protected ArrayList<Integer> TIME_STEPS;

	protected int MIN_STEP = 300; // Re-Crawl min 5 minutes

	protected int MAX_STEP = 365 * SECONDS_PER_DAY; // One year;

	public void setConf(Configuration conf) {
		super.setConf(conf);
		if (conf == null)
			return;

		TIME_STEPS_CONFIG = conf.getStrings("db.fetch.schedule.fixed.steps", "30d");
		TIME_STEPS = new ArrayList<Integer>();

		PeriodFormatter periodFormatter = new PeriodFormatterBuilder()
				.appendDays().appendSuffix("d")
				.appendHours().appendSuffix("h")
				.appendMinutes().appendSuffix("m")
				.appendSeconds().appendPrefix("")
				.toFormatter();

		for (String step : TIME_STEPS_CONFIG) {
			Period period = periodFormatter.parsePeriod(step);
			TIME_STEPS.add(period.toStandardSeconds().getSeconds());
		}
		System.out.println(TIME_STEPS.toString());

	}


	@Override
	public CrawlDatum setFetchSchedule(Text url, CrawlDatum page,
	                             long prevFetchTime, long prevModifiedTime, long fetchTime,
	                             long modifiedTime, int state) {

		System.out.println("FFS >> BEGIN SET FETCH SCHEDULE");

		super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime,
				fetchTime, modifiedTime, state);

		System.out.println(page.toString());

		int interval = page.getFetchInterval();
		long refTime = fetchTime;


		// https://issues.apache.org/jira/browse/NUTCH-1430
		interval = (interval == 0) ? TIME_STEPS.get(0) : interval;



		if (modifiedTime <= 0){
			modifiedTime = fetchTime;
			interval = TIME_STEPS.get(0);

			System.out.println("FFS >> NOT MODIFIED YET (NEW)");
			System.out.println("FFS >> SET modifiedTime: " + fetchTime);
			System.out.println("FFS >> SET INTERVAL: " + interval);
		}

		switch (state) {
			case FetchSchedule.STATUS_MODIFIED:

				interval = TIME_STEPS.get(0);
				System.out.println("FFS >> STATUS_MODIFIED");
				System.out.println("FFS >> SET INTERVAL: " + interval);
				break;
			case FetchSchedule.STATUS_NOTMODIFIED:
				int oldInterVal = TIME_STEPS.indexOf((int) interval);
				int nextInterval = oldInterVal + 1;
				//Next Interval always is last of set
				if (oldInterVal >= TIME_STEPS.size() - 1) nextInterval = oldInterVal;
				interval = TIME_STEPS.get(nextInterval);

				System.out.println("FFS >> STATUS_NOTMODIFIED");
				System.out.println("FFS >> SET INTERVAL: " + interval);

				break;
			case FetchSchedule.STATUS_UNKNOWN:
				System.out.println("FFS >> STATUS_UNKNOWN");
				break;
		}
		if (interval <= MIN_STEP) {
			interval = MIN_STEP;
		} else if (interval >= MAX_STEP) {
			interval = MAX_STEP;
		}


		System.out.println("Interval: " + interval);
		page.setFetchInterval(interval);
		page.setFetchTime(refTime + interval * 1000L);
		page.setModifiedTime(modifiedTime);
		return page;
	}

	/**
	 * This method provides information whether the page is suitable for selection
	 * in the current fetchlist. NOTE: a true return value does not guarantee that
	 * the page will be fetched, it just allows it to be included in the further
	 * selection process based on scores. The default implementation checks
	 * <code>fetchTime</code>, if it is higher than the
	 *
	 * @param curTime
	 *          it returns false, and true otherwise. It will also check that
	 *          fetchTime is not too remote (more than <code>maxInterval</code),
	 *          in which case it lowers the interval and returns true.
	 * @param url
	 *          URL of the page
	 * @param page
	 * @param curTime
	 *          reference time (usually set to the time when the fetchlist
	 *          generation process was started).
	 * @return true, if the page should be considered for inclusion in the current
	 *         fetchlist, otherwise false.
	 */
	@Override
	public boolean shouldFetch(Text url, CrawlDatum page, long curTime) {
		// pages are never truly GONE - we have to check them from time to time.
		// pages with too long fetchInterval are adjusted so that they fit within
		// maximum fetchInterval (batch retention period).
		long fetchTime = page.getFetchTime();
//		if (fetchTime - curTime > maxInterval * 1000L) {
//			if (page.getFetchInterval() > maxInterval) {
//				page.setFetchInterval(Math.round(maxInterval * 0.9f));
//			}
//			page.setFetchTime(curTime);
//		}
		System.out.println(">>>>>>> " + url +  " \t\t\t FETCH: " + (fetchTime <= curTime));
		return fetchTime <= curTime;
	}
}
