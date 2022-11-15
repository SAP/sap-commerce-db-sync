/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

// Parameters
queryToRun = 'select p_streetname from addresses where p_streetname like \'%Test%\''
indexToCreateaddresses = 'CREATE INDEX addresses_ownerpkstring_ddl ON ADDRESSES(typepkstring, ownerpkstring) ONLINE;'
indexToDropAddresses = 'drop index  addresses_ownerpkstring_ddl ONLINE;'
AlterAddColumnAddressesQuery = 'ALTER TABLE ADDRESSES ADD (TEST%s BIGINT) ONLINE;'
AlterRemoveColumnAddresses = 'ALTER TABLE ADDRESSES DROP (TEST%s) ONLINE;'

queryLoopCount = 600
threadSize = 1
threadPoolSize = 1  // Long Running Query Test

import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors
import groovy.time.TimeCategory
import de.hybris.platform.core.Registry
import de.hybris.platform.jalo.JaloSession
import de.hybris.platform.core.Tenant

queryTasks = []
Tenant currentTenant = Registry.getCurrentTenant();
// Create Callable Threads
1.upto(threadSize) { index ->
    // Create a database connection within each Thread
    queryTasks << {
        def totalTime1 = 0
        def totalTime2 = 0
        def indexCreation = true;
        // Each thread runs a thread/loop specific template query X times
        1.upto(queryLoopCount) { loopIndex ->

            try {
                Registry.setCurrentTenant(currentTenant);
                JaloSession.getCurrentSession().activate();

                start = new Date()
                totalTime2 = 0
                String AlterAddColumnAddresses = String.format(AlterRemoveColumnAddresses ,loopIndex);
                 println(AlterAddColumnAddresses)
                jdbcTemplate.execute(AlterAddColumnAddresses)
                stop = new Date()
                totalTime2 += TimeCategory.minus(stop, start).toMilliseconds()
                println "Table Column Creation  AlterAddColumnAddresses loop ${loopIndex} totalTime(ms)       ${totalTime2}"
                // Thread.sleep(5000);

            } finally {
                JaloSession.getCurrentSession().close();
                Registry.unsetCurrentTenant();
            }
        }
        // Return average as the result of the Callable
        totalTime1 + totalTime2 / queryLoopCount
    } as Callable
}

executorService = Executors.newFixedThreadPool(threadPoolSize)
println "Test started at ${new Date()}"
results = executorService.invokeAll(queryTasks)
totalAverage = 0
results.eachWithIndex { it, index ->
    totalAverage += it.get()
    println "$index --> ${it.get()}"
}
println "Total Average --> ${totalAverage / threadSize}"
println "Test finished at ${new Date()}"
executorService.shutdown()
executorService.awaitTermination(200, TimeUnit.SECONDS)