/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */
package com.sap.cx.boosters.commercedbsync.listeners;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.sap.cx.boosters.commercedbsync.constants.CommercedbsyncConstants;
import de.hybris.platform.jalo.type.TypeManager;
import de.hybris.platform.servicelayer.model.ModelService;
import de.hybris.platform.servicelayer.type.TypeService;
import de.hybris.platform.tx.AfterSaveEvent;
import de.hybris.platform.tx.AfterSaveListener;
import de.hybris.platform.util.Config;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.enums.ItemChangeType;
import com.sap.cx.boosters.commercedbsync.model.ItemDeletionMarkerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultCMTAfterSaveListener is an implementation of {@link AfterSaveListener} for use with
 * capturing changes to Delete operations for any configured data models.
 *
 */
public class DefaultCMTAfterSaveListener implements AfterSaveListener {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultCMTAfterSaveListener.class);

	private ModelService modelService;

	private static final String COMMA_SEPERATOR = ",";

	private TypeService typeService;

	private static final boolean deletionsEnabled = Config
			.getBoolean(CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES_ENABLED,false);


	@Override
	public void afterSave(final Collection<AfterSaveEvent> events) {
		if (!deletionsEnabled) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("CMT deletions is not enabled for ItemModel.");
			}
			return;
		}

		List<String> deletionsTypeCode = getListDeletionsTypeCode();

		if (deletionsTypeCode == null || deletionsTypeCode.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No typecode defined to create a deletion record for CMT ");
			}
			return;
		}
		events.forEach(event -> {
			{
				final int type = event.getType();
				final String typeCodeAsString = event.getPk().getTypeCodeAsString();
				if (AfterSaveEvent.REMOVE == type && deletionsTypeCode.contains(typeCodeAsString)) {
					final String tableName = TypeManager.getInstance()
							.getRootComposedType(event.getPk().getTypeCode()).getTable();
					final ItemDeletionMarkerModel idm = modelService.create(ItemDeletionMarkerModel.class);
					convertAndfillInitialDeletionMarker(idm, event.getPk().getLong(),
							tableName);
					modelService.save(idm);

				}
			}
		});

	}

	private void convertAndfillInitialDeletionMarker(final ItemDeletionMarkerModel marker, final Long itemPK,
			final String table)
	{
		Preconditions.checkNotNull(marker, "ItemDeletionMarker cannot be null in this place");
		Preconditions
				.checkArgument(marker.getItemModelContext().isNew(), "ItemDeletionMarker must be new");

		marker.setItemPK(itemPK);
		marker.setTable(table);
		marker.setChangeType(ItemChangeType.DELETED);
	}


	// TO DO change to static variable
	private List<String> getListDeletionsTypeCode() {
		final String typeCodes = Config.getString(
				CommercedbsyncConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES, "");
		if (StringUtils.isEmpty(typeCodes)) {
			return Collections.emptyList();
		}
		List<String> result = Splitter.on(COMMA_SEPERATOR)
				.omitEmptyStrings()
				.trimResults()
				.splitToList(typeCodes);

		return result;
	}

	public void setModelService(final ModelService modelService)
	{
		this.modelService = modelService;
	}

	public void setTypeService(TypeService typeService) {
		this.typeService = typeService;
	}
}
