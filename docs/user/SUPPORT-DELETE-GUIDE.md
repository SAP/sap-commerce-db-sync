# Commerce Database Sync - Deletion Support

SAP Commerce DB Sync does support deletions. It can be enabled for the transactional table using two different approaches:
- Default Approach using After Save Event Listener
- Alternative approach using Remove Interceptor

## Approaches for deletions

### Default Approach using After Save Event Listener

[After Save Event Listener](https://help.sap.com/viewer/d0224eca81e249cb821f2cdf45a82ace/2011/en-US/8b51226d866910149803df2610bb39a5.html), which will be enabled for the limited type code and only in a constraint violation.
* Activate the After save listener by defining the implementation of AfterSaveListener interface.

```
<bean id="defaultCMTAfterSaveListener" class="org.sap.commercemigration.listeners.DefaultCMTAfterSaveListener">
    <property name="modelService" ref="modelService" />
    <property name="typeService" ref="typeService" />
  </bean>
  ```

* Configurable property for the list of type codes where we should manage deletion
```
  # Provide the typecodes in comma seperated
  migration.data.incremental.deletions.typecodes=4,30
  migration.data.incremental.deletions.typecodes.enabled=true
```
* Dedicated item type for deleted records (separate table with PK).
```
For now, it is supported through **ItemDeletionMarker**.
```
* Deletion activity is tied with incremental to avoid duplicates.

**Disclaimer**: It will not support for the direct deletions via DB or JDBC Template. When SAP Commerce server is stopped, events published in the queue and that have not been handle by **AfterSaveEventPublisher** threads are lost. 

#### Technical Concept

##### Publish after save event
When a transaction is committed, an after save event is either added to a blocking queue in case of asynchronous mode or notify directly After Save Listeners in case of synchronous mode.
![Publish after save event](after_save_listener_1.png)

##### Event handling when event sent asynchronously

The pool of **AfterSaveEventPublisherThread** is managed by **DefaultAfterSaveListenerRegistry**, each thread drains the blocking queue of after send event and call After Save Listeners.

![Event Handle asynchronously](after_save_listener_2.png)

Here are the **DefaultAfterSaveListenerRegistry** tuning parameters:
```
core.aftersave.async=true //default true (asynchronous mode)
core.aftersave.interval=200 //sleep time in ms
core.aftersave.batchsize=1024 //draining batch size
core.aftersave.queuesize=1024 //maximum elements in the queue before blocking
```

### Alternative approach using Remove Interceptor

**Note**: It is disabled by default, only enable if you face difficulties with _after save listener_ approach.

Remove Interceptor which will be enabled for the limited ItemTypes and only in a constraint violation.
* Activate the Delete Interceptor by defining an InterceptorMapping for each tracked item type.

```<bean id="defaultCMTRemoveInterceptorMapping"
    class="de.hybris.platform.servicelayer.interceptor.impl.InterceptorMapping">
    <property name="interceptor" ref="defaultCMTRemoveInterceptor"/>
    <property name="typeCode" value="Item" />
  </bean>
  ```

* Configurable property for the list of type codes where we should manage deletion
```
  # Provide the itemType for deletions
  migration.data.incremental.deletions.itemtype=Media,Employee
  migration.data.incremental.deletions.itemtypes.enabled=true
```
* Dedicated item type for deleted records (separate table with PK).
```
For now, it is supported by ItemDeletionMarker.
```
* Deletion activity is tied with incremental to avoid duplicates.

**Disclaimer**: Deletions will work with SL (legacy sync, legacy Impex, Service Layer Direct)
## When to use

* Not required to be enabled for all the tables and few use-cases could be considered
  - In case of Constraint validation failure
  - Deletion is triggered by application, e.g. removing the entry from a cart.
* Don't enable for audit table or task logs
* It is covering deletions and migration together to avoid constraint validation.
* It can be toggle through properties.
