import boto3
import sys

from lambda_helper import *


class SageMakerCleanup:
    def __init__(self, logging, whitelist, settings, resource_tree, region):
        self.logging = logging
        self.whitelist = whitelist
        self.settings = settings
        self.resource_tree = resource_tree
        self.region = region
        
        try:
            self.client = boto3.client('sagemaker', region_name=region)
        except:
            self.logging.error(str(sys.exc_info()))
    
    
    def run(self):
        self.notebooks()
        self.endpoints()
        
    
    def notebooks(self):
        """
        Deletes SageMaker Notebooks.
        """

        clean = self.settings.get('services').get('sagemaker', {}).get('notebooks', {}).get('clean', False)
        if clean:
            try:
                resources = self.client.list_notebook_instances().get('NotebookInstances')
            except:
                self.logging.error(str(sys.exc_info()))
                return None
            
            ttl_days = self.settings.get('services').get('sagemaker', {}).get('notebooks', {}).get('ttl', 7)
            
            for resource in resources:
                resource_id = resource.get('NotebookInstanceName')
                resource_date = resource.get('LastModifiedTime')
                resource_status = resource.get('NotebookInstanceStatus')

                if resource_id not in self.whitelist.get('sagemaker', {}).get('notebook', []):
                    delta = LambdaHelper.get_day_delta(resource_date)
                
                    if delta.days > ttl_days:
                        if resource_status == 'InService':
                            if not self.settings.get('general', {}).get('dry_run', True):
                                try:
                                    self.client.describe_notebook_instance(NotebookInstanceName=resource_id)
                                except:
                                    self.logging.error("Could not delete SageMaker Notebook '%s'." % resource_id)
                                    self.logging.error(str(sys.exc_info()))
                                    continue

                            self.logging.info(("SageMaker Notebook '%s' was last modified %d days ago "
                                               "and has been deleted.") % (resource_id, delta.days))
                        else:
                            self.logging.debug("SageMaker Notebook '%s' in state '%s' cannot be deleted." % (resource_id, resource_status))
                    else:
                        self.logging.debug(("SageMaker Notebook '%s' was created %d days ago "
                                            "(less than TTL setting) and has not been deleted.") % (resource_id, delta.days))
                else:
                    self.logging.debug("SageMaker Notebook '%s' has been whitelisted and has not been deleted." % (resource_id))
                
                self.resource_tree.get('AWS').setdefault(
                    self.region, {}).setdefault(
                        'SageMaker', {}).setdefault(
                            'Notebooks', []).append(resource_id)
        else:
            self.logging.debug("Skipping cleanup of SageMaker Notebook.")
    
    
    def endpoints(self):
        """
        Deletes SageMaker Endpoints.
        """

        clean = self.settings.get('services').get('sagemaker', {}).get('endpoints', {}).get('clean', False)
        if clean:
            try:
                resources = self.client.list_endpoints().get('Endpoints')
            except:
                self.logging.error(str(sys.exc_info()))
                return None
            
            ttl_days = self.settings.get('services').get('sagemaker', {}).get('endpoints', {}).get('ttl', 7)
            
            for resource in resources:
                resource_id = resource.get('EndpointName')
                resource_date = resource.get('LastModifiedTime')
                resource_status = resource.get('EndpointStatus')

                if resource_id not in self.whitelist.get('sagemaker', {}).get('endpoint', []):
                    delta = LambdaHelper.get_day_delta(resource_date)
                
                    if delta.days > ttl_days:
                        if resource_status == 'InService':
                            if not self.settings.get('general', {}).get('dry_run', True):
                                try:
                                    self.client.delete_endpoint(EndpointName=resource_id)
                                except:
                                    self.logging.error("Could not delete SageMaker Endpoint '%s'." % resource_id)
                                    self.logging.error(str(sys.exc_info()))
                                    continue

                            self.logging.info(("SageMaker Endpoint '%s' was created %d days ago "
                                               "and has been deleted.") % (resource_id, delta.days))
                        else:
                            self.logging.debug("SageMaker Endpoint '%s' in state '%s' cannot be deleted." % (resource_id, resource_status))
                    else:
                        self.logging.debug(("SageMaker Endpoint '%s' was created %d days ago "
                                            "(less than TTL setting) and has not been deleted.") % (resource_id, delta.days))
                else:
                    self.logging.debug("SageMaker Endpoint '%s' has been whitelisted and has not been deleted." % (resource_id))
                
                self.resource_tree.get('AWS').setdefault(
                    self.region, {}).setdefault(
                        'SageMaker', {}).setdefault(
                            'Endpoints', []).append(resource_id)
        else:
            self.logging.debug("Skipping cleanup of SageMaker Endpoints.")