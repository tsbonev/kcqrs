package com.clouway.kcqrs.core

/**
 * Repository is representing an Repository which operates with the AggregateRoot objects.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface AggregateRepository {

    /**
     * Creates a new or updates an existing aggregate in the repository.
     *
     * @param aggregate the aggregate to be registered
     * @throws EventCollisionException is thrown in case of
     */
    @Throws(PublishErrorException::class, EventCollisionException::class)
    fun <T : AggregateRoot> save(aggregate: T)

    /**
     * Get the aggregate
     *
     * @param id
     * @return
     * @throws HydrationException
     * @throws AggregateNotFoundException
     */
    @Throws(HydrationException::class, AggregateNotFoundException::class)
    fun <T : AggregateRoot> getById(id: String, type: Class<T>): T

    /**
     * Get a set of aggregates by providing a list of ids.
     *
     * @param ids the list of ID's
     * @return a list of aggregates or empty list if none of them is matching
     * @throws HydrationException
     */
    @Throws(HydrationException::class)
    fun <T : AggregateRoot> getByIds(ids: List<String>, type: Class<T>): Map<String, T>

}